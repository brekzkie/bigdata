from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'crypto-pipeline',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='crypto_auto_etl',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',  # bisa ganti @daily / */30 * * * *
    catchup=False
) as dag:

    run_collector = BashOperator(
        task_id='run_crypto_collector',
        bash_command='python /app/main.py'
    )

    run_collector
