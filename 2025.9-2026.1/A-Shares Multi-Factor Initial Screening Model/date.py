import baostock as bs
import tushare as ts
import pandas as pd
import sqlite3
import random
from datetime import datetime, timedelta
import warnings
from dotenv import load_dotenv
import os
import pymysql
from pymysql import Error

warnings.filterwarnings('ignore')
load_dotenv("D:\\KEY\\Key.env")

#配置密钥
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
#配置时间范围 2年
END_DATE = datetime.now().strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(days=365*2)).strftime("%Y-%m-%d")
#抽取数量
STOCK_NUM = 30
#数据库配置
DB_TYPE = "sqlite"
SQLITE_DB_NAME = "stock_data.db"
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": "stock_db",
    "charset": "utf8mb4"
}

def init_apis():
    """
    init_apis 的 Docstring 初始化Baostock和Tushare接口，返回Tushare pro对象
    """
    lg = bs.login()
    if lg.error_code != '0':
        raise Exception(f"Baostock登录失败：{lg.error_msg}")
    ts.set_token(TUSHARE_TOKEN)
    try:
        pro = ts.pro_api()
    except Exception as e:
        raise Exception(f"Tushare初始化失败：{e}")
    
    return pro

def get_hs300_stocks():
    """
    get_hs300_stocks 的 Docstring 从沪深300 中抽30只
    """
    #获取全部沪深300成分股
    rs = bs.query_hs300_stocks()
    stock_list = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        stock_list.append(row)
    if not stock_list:
        raise Exception("沪深300获取失败")
    
    stock_df = pd.DataFrame(stock_list, columns=rs.fields)
    #随机抽30只
    selected_stcoks = stock_df.sample(n=STOCK_NUM, random_state=10111213)["code"].tolist()
    print(f"成功随机30只 {selected_stcoks}")
    return selected_stcoks

def get_stock_combined_data(stock_codes):
    """
    合并获取日线数据和财务数据(PE/PB),一次调用Baostock接口，效率更高
    """
    #日线和财务
    combined_fields = "date,code,open,high,low,close,volume,amount,peTTM,pbMRQ"
    combined_data = []

    for code in stock_codes:
        # 过滤无效代码
        if code in ["sh.302132", "sz.302132"]:
            print(f"跳过无效代码 {code}")
            continue
        print(f"正在获取{code} 日线+财务数据")

        try:
            rs = bs.query_history_k_data_plus(
                code=code,
                fields=combined_fields, 
                start_date=START_DATE,
                end_date=END_DATE,
                frequency="d",
                adjustflag="3"  #后复权
            )

            # 验证接口返回状态
            if rs.error_code != '0':
                print(f"{code} 接口调用失败：{rs.error_msg}")
                continue

            stock_data = []
            while rs.next():
                stock_data.append(rs.get_row_data())

            # 格式调整
            if stock_data:
                df = pd.DataFrame(stock_data, columns=combined_fields.split(","))
                df["date"] = pd.to_datetime(df["date"], errors='coerce')
                df = df[df["date"].notna()]
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")

                numeric_cols = ["open", "high", "low", "close", "volume", "amount", "peTTM", "pbMRQ"]
                df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
                df = df.fillna(0)
                df.rename(columns={"peTTM": "pe", "pbMRQ": "pb"}, inplace=True)

                combined_data.append(df)

        except Exception as e:
            print(f"{code} 数据获取失败：{e}")
            continue

    # 拼接所有数据
    if combined_data:
        combined_df = pd.concat(combined_data, ignore_index=True)
        daily_df = combined_df[["date", "code", "open", "high", "low", "close", "volume", "amount"]].copy()
        finance_df = combined_df[["date", "code", "pe", "pb"]].copy()

        print(f"日线数据获取成功 共{len(daily_df)}条记录")
        print(f"财务因子获取成功 共{len(finance_df)}条记录")

        return daily_df, finance_df
    else:
        raise Exception("未获取到任何日线或财务数据")
    #转化数值
    
def store_to_db(daily_df, finance_df):
    """
    store_to_db 的 Docstring 合并日线和财务因子 并保存到数据库
    """
    #合并
    merged_df = pd.merge(daily_df, finance_df, on=["code", "date"], how="left")
    merged_df.fillna(0, inplace=True)
    print(f"数据已整合完成 共{len(merged_df)}条")

    #选择sqlite/mysql
    if DB_TYPE == "sqlite":
        conn = sqlite3.connect(SQLITE_DB_NAME)
        cursor = conn.cursor()
        print(f"链接数据库SQLite成功 {SQLITE_DB_NAME}")
    else:
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()
            print(f"链接数据库MySQL成功 {MYSQL_CONFIG['database']}")
        except Error as e:
            raise Exception(f"链接失败 {e}")
        
    #创建数据库表
    if DB_TYPE == "sqlite":
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS stock_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code VARCHAR(20) NOT NULL,
            date VARCHAR(10) NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            amount FLOAT,
            pe FLOAT,
            pb FLOAT,
            UNIQUE(code, date)
        );
        """
    else:
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS stock_daliy(
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            code VARCHAR(20) NOT NULL,
            date VARCHAR(20) NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            amount FLOAT,
            pe FLOAT,
            pb FLOAT,
            UNIQUE(code, date)
        );        
        """
    cursor.execute(create_table_sql)
    conn.commit()

    #插入数据列
    insert_sql_temple = """
    INSERT OR REPLACE INTO stock_daily
    (code, date, open, high, low, close, volume, amount, pe, pb)
    VALUES ({placeholders});
    """
    #sqlite/mysql 不同占位符
    if DB_TYPE == "sqlite":
        insert_sql = insert_sql_temple.format(placeholders="?, ?, ?, ?, ?, ?, ?, ?, ?, ?")
    else:
        insert_sql = insert_sql_temple.replace("INSERT OR REPLACE", "INSERT IGNORE").format(placeholders="%s, %s, %s, %s, %s, %s, %s, %s, %s, %s")

    insert_date = merged_df[["code", "date", "open", "high", "low", "close", "volume", "amount", "pe", "pb"]].values.tolist()
    #插入
    cursor.executemany(insert_sql, insert_date)
    conn.commit()
    print(f"插入成功 共{len(insert_date)}条插入{DB_TYPE}")

    #总记录
    cursor.execute("SELECT COUNT(*) FROM stock_daily;")
    total_count = cursor.fetchone()[0]
    print(f"总记录数为{total_count}")

    cursor.close()
    conn.close()

def main():
    """
    main 的 Docstring 调用所有函数
    """
    try:
        pro = init_apis()
        stock_codes = get_hs300_stocks()
        daily_df, finance_df = get_stock_combined_data(stock_codes)
        store_to_db(daily_df, finance_df)
        bs.logout()
        print("\n 所有流程运行成功！")
    except Exception as e:
        print(f"\n 运行失败！ {e}")
        bs.logout()

if __name__ == "__main__":
    main()

