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
load_dotenv()

#配置密钥
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
#配置时间范围 2年
END_DATE = datetime.now().strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(day=365*2)).strftime("%Y-%m-%d")
#抽取数量
STOCK_NUM = 30
#数据库配置
DB_TYPE = "sqlite"
SQLITE_DB_NAME = "stock_data.db"
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "stock_db",
    "charset": "utf8mb4"
}

def init_apis():