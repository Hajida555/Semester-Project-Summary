import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3

#================================================================
#全局配置
#================================================================
plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False

#================================================================
#读取数据
#================================================================
def read_data_from_db(db_name="stock_data.db"):
    try:
        conn = sqlite3.connect(db_name)
        df = pd.read_sql_query("SELECT * FROM stock_daily", conn)
        conn.close()

        #初步数据清理
        df = df.fillna(0)
        df = df[df["close"] > 0]
        df["date"] = pd.to_datetime(df["date"]) 

        print(f"读取成功 共{len(df)} 条")
        return df
    except Exception as e:
        raise Exception(f"读取失败 {e}")

#================================================================
#微积分计算
#================================================================
def calculate_stable_stocks(df):
    stable_stock_list = []
    for code, group_df in df.groupby("code"):
        group_df = group_df.sort_values(by="date").reset_index(drop=True)

        #计算日收益率 瞬时变化率 并删除第一行的缺失值
        group_df["daily_return"] = group_df["close"].pct_change()
        group_df = group_df.dropna(subset=["daily_return"])

        group_df["return_derivative"] = group_df["daily_return"].diff()
        group_df = group_df.dropna(subset=["return_derivative"])

        #通过瞬时变化率均值筛选出稳定的标的
        avg_abs_derivative = np.mean(np.abs(group_df["return_derivative"]))
        if avg_abs_derivative <= 0.05:
            stable_stock_list.append(code)
            print(f"{code}波动稳定 变化率平均值：{avg_abs_derivative:.4f}")
        
    stable_df = df[df["code"].isin(stable_stock_list)].reset_index(drop=True)
    if stable_df.empty:
        raise Exception("无符合的标的")
    
    print(f"\n一共有{len(stable_stock_list)}只符合的标的")

    return stable_df, stable_stock_list

#================================================================
#线性代数计算
#================================================================
def calculate_factor_score(stable_df):
    stable_df_copy = stable_df.copy()

    #因子数据标准化
    factor_cols = ["pe", "pb"]
    for factor in factor_cols:
        mean = stable_df_copy[factor].mean()
        std = stable_df_copy[factor].std()
        stable_df_copy[f"{factor}_standardized"] = (stable_df_copy[factor]- mean) / (std + 1e-8)

    #导入PE/PB的数据
    factor_standardized_cols = ["pe_standardized", "pb_standardized"]
    factor_matrix = stable_df_copy[factor_standardized_cols].values

    #权重矩阵
    weight_matrix = np.array([[0.5], [0.5]])

    #相乘取平均值
    stable_df_copy["comprehensive_score"] = np.dot(factor_matrix, weight_matrix).flatten()
    stock_score_df = stable_df_copy.groupby("code")["comprehensive_score"].mean().reset_index()
    stock_score_df = stock_score_df.sort_values(by="comprehensive_score", ascending=False).reset_index(drop=True)

    print(f"\n得分前10股票{stock_score_df.head(10)}")

    return stable_df_copy, stock_score_df

#================================================================
#对比等权选股与矩阵加权选股的收益差异 plt绘制收益曲线
#================================================================
def compare_strategy_returns(stable_df_copy, stock_score_df):
    #确定股票
    top_n=10
    top_stocks = stock_score_df.head(top_n)["code"].tolist()
    all_stable_stocks = stock_score_df["code"].tolist()

    #计算两种股票的每日收益
    date_grouped = stable_df_copy.groupby("date")
    date_list = sorted(list(date_grouped.groups.keys()))
    strategy_returns = {
        "equal_weight": [0.0],
        "matrix_weight": [0.0]
    }

    for i in range(1, len(date_list)):
        prev_date = date_list[i-1]
        curr_date = date_list[i]

        prev_date_df = stable_df_copy[stable_df_copy["date"] == prev_date]
        curr_date_df = stable_df_copy[stable_df_copy["date"] == curr_date]

        #等权选股收益
        equal_weight_return = 0.0
        common_stocks = set(prev_date_df["code"]).intersection(set(curr_date_df["code"]))
        if common_stocks and common_stocks.issubset(all_stable_stocks):
            for stock in common_stocks:
                prev_close = prev_date_df[prev_date_df["code"] == stock]["close"].values[0]
                curr_close = curr_date_df[curr_date_df["code"] == stock]["close"].values[0]
                stock_return = (curr_close - prev_close) / prev_close
                equal_weight_return += stock_return /len(common_stocks)
        strategy_returns["equal_weight"].append(strategy_returns["equal_weight"][-1] + equal_weight_return)

        #矩阵加权收益
        matrix_weight_return = 0.0
        top_common_stocks = set(prev_date_df["code"]).intersection(set(top_stocks))
        if top_common_stocks:
            for stock in top_common_stocks:
                prev_close = prev_date_df[prev_date_df["code"] == stock]["close"].values[0]
                curr_close = curr_date_df[curr_date_df["code"] == stock]["close"].values[0]
                stock_return = (curr_close - prev_close) / prev_close
                matrix_weight_return += stock_return / len(top_common_stocks)
        strategy_returns["matrix_weight"].append(strategy_returns["matrix_weight"][-1] + matrix_weight_return)

    #折线表对比
    plt.figure(figsize=(14, 7))
    plt.plot(date_list, strategy_returns["equal_weight"], label="等权选股", color="blue", linewidth=2)
    plt.plot(date_list, strategy_returns["matrix_weight"], label="PE/PB选股", color="red", linewidth=2)
    plt.title("等权 vs 矩阵", fontsize=16)
    plt.xlabel("日期", fontsize=12)
    plt.ylabel("累计收益", fontsize=12)
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    #输出最终结果
    final_equal_return = strategy_returns["equal_weight"][-1]
    final_matrix_return = strategy_returns["matrix_weight"][-1]
    print(f"\n两种策略对比：")
    print(f"等权最终收益:{final_equal_return:.4f}")
    print(f"矩阵最终收益:{final_matrix_return:.4f}")

#================================================================
#组函数
#================================================================
def main_calculation():
    try:
        stock_df = read_data_from_db()
        stable_df, stable_stock_list = calculate_stable_stocks(stock_df)
        stable_df_copy, stock_score_df = calculate_factor_score(stable_df)
        compare_strategy_returns(stable_df_copy, stock_score_df)
    
        print("\n运行成功！")
    except Exception as e:
        print(f"运行失败！{e}")

if __name__ == "__main__":
    main_calculation()