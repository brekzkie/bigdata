from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def fetch_and_analyze_news():
    print("✅ Fetching and analyzing news sentiment...")
    # Tambahkan logika kamu di sini

with DAG(
    'news_sentiment_analysis',
    start_date=datetime(2025, 12, 9),
    schedule_interval='@hourly',  # tiap jam
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    analyze_task = PythonOperator(
        task_id='analyze_news_sentiment',
        python_callable=fetch_and_analyze_news,
    )