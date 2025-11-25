from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from pymongo import MongoClient
import os

COIN_ID = "bitcoin"
VS_CURRENCY = "usd"
DAYS = "1"

def fetch_and_save():
    url = f"https://api.coingecko.com/api/v3/coins/{COIN_ID}/market_chart"
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    response = requests.get(
        url,
        params={"vs_currency": VS_CURRENCY, "days": DAYS},
        headers=headers
    )

    data = response.json()
    prices = data["prices"]

    df = pd.DataFrame(prices, columns=["timestamp", "price"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["coin_id"] = COIN_ID
    df = df[["datetime", "coin_id", "price"]]

    client = MongoClient("mongodb", 27017)
    db = client["cryptodb"]
    collection = db["coin_prices"]

    collection.insert_many(df.to_dict("records"))
    print("✅ ETL sukses masuk MongoDB")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="crypto_etl_auto",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",  # tiap 5 menit
    catchup=False
) as dag:

    etl_task = PythonOperator(
        task_id="fetch_crypto_to_mongo",
        python_callable=fetch_and_save
    )

    etl_task
