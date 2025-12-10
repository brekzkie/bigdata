from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def run_stock_etl():
    print("✅ Running ETL for stock data...")
    # Panggil fungsi dari collector/stock_fetcher.py atau processing/

with DAG(
    'stock_etl_pipeline',
    start_date=datetime(2025, 12, 9),
    schedule_interval='0 8 * * *',  # setiap hari pukul 08:00
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id='extract_transform_load',
        python_callable=run_stock_etl,
    )