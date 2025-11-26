from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from processing.feature_engineering import build_features
from pymongo import MongoClient
import pandas as pd

def extract_from_mongo():
    client = MongoClient("mongodb://mongo:27017")
    data = list(client.stock_db.prices.find())
    df = pd.DataFrame(data)
    df.to_csv("/tmp/raw.csv", index=False)

def transform():
    df = pd.read_csv("/tmp/raw.csv")
    df_feat = build_features(df)
    df_feat.to_csv("/tmp/features.csv", index=False)

with DAG(
    dag_id="stock_etl_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=extract_from_mongo)
    transform = PythonOperator(task_id="transform", python_callable=transform)

    extract >> transform
