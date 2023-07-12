import airflow
import pendulum
import requests 
import pandas as pd
from datetime import datetime, date, timedelta
import time
from binance.client import Client
import pandas as pd

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

import os
from dotenv import load_dotenv

load_dotenv()

ERP_CHANGE_DATE = airflow.utils.dates.days_ago(1)

api_key, api_secret = os.getenv('API_KEY'), os.getenv('API_SECRET')

def _binance_api():
    client = Client(api_key, api_secret)
    symbols = client.get_all_tickers()

    # interval = ["5MINUTE", "1HOUR", "4HOUR", "1DAY", "1WEEK", "1MONTH"]
    symbols_usdt = []
    for i in symbols:
        if 'USDT' in i['symbol']:
            symbols_usdt.append(i['symbol'])
    symbols = symbols_usdt

    # interval = ["5MINUTE", "1HOUR", "4HOUR", "1DAY", "1WEEK", "1MONTH"]

    intervals = ['1MONTH']
    for symbol in symbols[3:6]:
        # ETHBTC : 5m, 1h, 4h, 1d, 1w, 1M
        for interval in intervals:
            klines = client.get_historical_klines(symbol, getattr(Client, f"KLINE_INTERVAL_{interval}"), "2 Jul, 2022", "4 Jul, 2023")
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                                                'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignored'])
            df = df.iloc[:,:6]
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')


            # Save the DataFrame as a CSV file
            file_name = f"{symbol.lower()}_{interval.lower()}.csv"
            df.to_csv(f'/home/airflow/data/{file_name}', index=False)

with DAG(
    dag_id='binance',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@daily',
) as dag:
    start = DummyOperator(task_id='start')

    binance_api = PythonOperator(task_id='binance_crawl', python_callable=_binance_api)

    start >> binance_api
