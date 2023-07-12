import airflow
import pendulum
import requests 
import pandas as pd
from datetime import datetime, date, timedelta
import time
from urllib import parse
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

ERP_CHANGE_DATE = airflow.utils.dates.days_ago(1)

head = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0"}

def _data_gathering(**context):
    if context["execution_date"] <= ERP_CHANGE_DATE:
        return "past_tesla"
    else:
        return "now_tesla"

def _past_tesla():
    url = "https://query1.finance.yahoo.com/v8/finance/chart/TSLA?formatted=true&crumb=uQj.lsPu5Gf&lang=en-US&region=US&includeAdjustedClose=true&interval=1d&period1=1669766400&period2=1685404800&events=capitalGain|div|split&useYfid=true&corsDomain=finance.yahoo.com"
    result = parse.urlparse(url)
    # query부분을 dict 형태로 변환 
    result_dict = parse.parse_qs(result.query)
    # dict 형태의 쿼리에 해당 날짜 값을 수정 
    date_time = datetime(2011, 1, 1, 0, 0)

    result_dict['period1'] = [str(int(time.mktime(date_time.timetuple())))]
    # 분해했던 주소 값들을 다시 조립
    api_url = parse.ParseResult(scheme  = result.scheme, netloc= result.hostname, path=result.path, 
                    params = result.params, query=parse.urlencode(result_dict, doseq=True), fragment=result.fragment)
    # 조립된 값을 urlparse를 사용하여 실제 주소로 변경 
    r = requests.get(parse.urlunparse(api_url), headers=head)
    df = pd.DataFrame(r.json()['chart']['result'][0]['indicators']['quote'][0])
    df['timestamp'] = r.json()['chart']['result'][0]['timestamp']
    df['date'] = df['timestamp'].apply(datetime.fromtimestamp)
    df.to_csv("/home/airflow/data/tesla_past.csv", index=False)


def _now_tesla():
    url = "https://query1.finance.yahoo.com/v8/finance/chart/TSLA?formatted=true&crumb=uQj.lsPu5Gf&lang=en-US&region=US&includeAdjustedClose=true&interval=1d&period1=1669766400&period2=1685404800&events=capitalGain|div|split&useYfid=true&corsDomain=finance.yahoo.com"
    result = parse.urlparse(url)
    # query부분을 dict 형태로 변환 
    result_dict = parse.parse_qs(result.query)
    # dict 형태의 쿼리에 해당 날짜 값을 수정 
    date_time2= date.today()
    date_time1 = date.today() - timedelta(days=1)

    result_dict['period1'] = [str(int(time.mktime(date_time1.timetuple())))]
    result_dict['period2'] = [str(int(time.mktime(date_time2.timetuple())))]
    # 분해했던 주소 값들을 다시 조립
    api_url = parse.ParseResult(scheme  = result.scheme, netloc= result.hostname, path=result.path, 
                    params = result.params, query=parse.urlencode(result_dict, doseq=True), fragment=result.fragment)
    # 조립된 값을 urlparse를 사용하여 실제 주소로 변경 
    r = requests.get(parse.urlunparse(api_url), headers=head)
    df = pd.DataFrame(r.json()['chart']['result'][0]['indicators']['quote'][0])
    df['timestamp'] = r.json()['chart']['result'][0]['timestamp']
    df['date'] = df['timestamp'].apply(datetime.fromtimestamp)
    df.to_csv("/home/airflow/data/tesla_now.csv", index=False)
    
    
def _past_sox():
    pages = 50
    data = {"symbol" : "NAS@SOX",
        "fdtc" : "0",
        "page" : "3",}
    
    url = "https://finance.naver.com/world/worldDayListJson.naver?"
    total = []
    for x in range(1,pages):
        data['page'] = x
        total.append(pd.DataFrame(requests.post(url, data=data, headers=head).json()))
    sox_df = pd.concat(total, ignore_index=True)        
    
    total = []
    for x in range(1,pages):
        data['page'] = x
        data['symbol'] = "SPI@SPX"
        total.append(pd.DataFrame(requests.post(url, data=data, headers=head).json()))
    spx_df = pd.concat(total, ignore_index=True) 
    
    sox_df.to_csv("/home/airflow/data/sox_past.csv", index=False)
    spx_df.to_csv("/home/airflow/data/spx_past.csv", index=False)

def _now_sox():
    data = {"symbol" : "NAS@SOX",
        "fdtc" : "0",
        "page" : "3",}
    
    url = "https://finance.naver.com/world/worldDayListJson.naver?"
    pd.DataFrame(requests.post(url, data=data, headers=head).json()).iloc[0:1, :].to_csv("/home/airflow/data/now_sox.csv", index=False)
    
    data['symbol'] = "SPI@SPX"
    pd.DataFrame(requests.post(url, data=data, headers=head).json()).iloc[0:1, :].to_csv("/home/airflow/data/now_spx.csv", index=False)
     
    
def _latest_only(**context):
    now = pendulum.now("UTC")
    left_window = context["dag"].following_schedule(context["execution_date"])
    right_window = context["dag"].following_schedule(left_window)

    if not left_window < now <= right_window:
        raise AirflowSkipException()


with DAG(
    dag_id="tesla",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    pick_erp = BranchPythonOperator(
        task_id="data_gathering", python_callable=_data_gathering
    )
    
    past_tesla = PythonOperator(task_id="past_tesla", python_callable=_past_tesla)
    now_tesla = PythonOperator(task_id="now_tesla", python_callable=_now_tesla)

    past_sox = PythonOperator(task_id="past_sox", python_callable=_past_sox)
    now_sox = PythonOperator(task_id="now_sox", python_callable=_now_sox)
    join_data = DummyOperator(task_id="join_data", trigger_rule="none_failed")
    eda_data = DummyOperator(task_id="eda")
    train = DummyOperator(task_id="train")
    
    
    start >> pick_erp
    pick_erp >> [past_tesla, now_tesla]
    past_tesla >> past_sox
    now_tesla >> now_sox
    [now_sox, past_sox] >> join_data
    join_data >> eda_data >> train


