from datetime import datetime,timedelta
import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from datacrawl import get_crypto_trading_data,get_tweet_and_analysis_sentiment
from LSTMModel import train_model
from datetime import date

from airflow import DAG
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'VMD',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}



with DAG(
    default_args = default_args,
    dag_id = 'crypto_data_etl_auto_train_model',
    description = 'dag get crypto data and train predictive model for next 24h hours',
    start_date = datetime(2022,12,25,7),
    schedule_interval = '0 7 * * *'
) as dag:
    task1 = PythonOperator(
        task_id = 'get_crypto_trading_data',
        python_callable = get_crypto_trading_data,
    )
    task2 = PythonOperator(
        task_id = 'get_tweet_and_analysis_sentiment',
        python_callable = get_tweet_and_analysis_sentiment,
    )
    task3 = PythonOperator(
        task_id = 'train_model',
        python_callable = train_model
    )

    task1,task2 >> task3