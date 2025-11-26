from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="stock_auto_etl",
    default_args=default_args,
    description="Auto ETL saham ke MongoDB",
    schedule_interval="0 1 * * *",  # tiap hari jam 1 pagi
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    run_collector = BashOperator(
        task_id="run_stock_collector",
        bash_command="docker exec stock_prices_collector python main.py"
    )

    run_collector
