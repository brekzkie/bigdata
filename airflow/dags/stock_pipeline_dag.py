import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- KONFIGURASI PATH (PENTING) ---
# Menambahkan folder root project ke sistem agar Airflow bisa membaca script Anda
# /opt/airflow/dags/stock_pipeline_dag.py -> naik 2 level -> /opt/airflow
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, '../../'))
sys.path.insert(0, root_dir)

# --- IMPORT MODULE ---
# Kita bungkus dalam try-except agar tidak bikin DAG error total kalau ada modul kurang
try:
    from collector.stock_fetcher import run_fetch_stocks
    from collector.news_fetcher import run_fetch as run_fetch_news
    from collector.sentiment import run_sentiment_analysis
    from processing.feature_engineering import run_feature_engineering_pipeline
    from processing.forecasting import run_forecasting_pipeline
except ImportError as e:
    print(f"⚠️ Import Warning: {e}")
    # Dummy function biar DAG tetap muncul di UI walau error
    def run_fetch_stocks(): print("Error import stock")
    def run_fetch_news(): print("Error import news")
    def run_sentiment_analysis(): print("Error import sentiment")
    def run_feature_engineering_pipeline(): print("Error import FE")
    def run_forecasting_pipeline(): print("Error import forecast")

# --- WRAPPER FUNCTIONS ---
def task_fetch_stocks_wrapper():
    print("🚀 [TASK 1] Mengambil Data Saham...")
    run_fetch_stocks()

def task_fetch_news_wrapper():
    print("📰 [TASK 2] Mengambil Berita & Sentimen...")
    run_fetch_news()
    run_sentiment_analysis() # Langsung jalankan sentimen setelah berita

def task_process_data_wrapper():
    print("⚙️ [TASK 3] Feature Engineering...")
    run_feature_engineering_pipeline()

def task_train_predict_wrapper():
    print("🔮 [TASK 4] Forecasting (Training AI)...")
    run_forecasting_pipeline()

# --- DEFINISI DAG ---
default_args = {
    'owner': 'gemini_user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_automation_pipeline',
    default_args=default_args,
    description='Pipeline Big Data Saham (Setiap 4 Jam)',
    
    # === JADWAL: Setiap 4 Jam (00:00, 04:00, 08:00, dst) ===
    schedule_interval='0 */4 * * *', 
    
    start_date=datetime(2025, 12, 10),
    catchup=False,
    tags=['bigdata', 'stock', 'ai'],
) as dag:

    # 1. Fetch Saham
    t1 = PythonOperator(
        task_id='fetch_stock_prices',
        python_callable=task_fetch_stocks_wrapper,
    )

    # 2. Fetch Berita + Sentimen
    t2 = PythonOperator(
        task_id='fetch_news_sentiment',
        python_callable=task_fetch_news_wrapper,
    )

    # 3. Feature Engineering (Butuh data saham & berita dulu)
    t3 = PythonOperator(
        task_id='feature_engineering',
        python_callable=task_process_data_wrapper,
    )

    # 4. Forecasting
    t4 = PythonOperator(
        task_id='ai_forecasting',
        python_callable=task_train_predict_wrapper,
    )

    # --- URUTAN KERJA (Parallel & Sequential) ---
    # Fetch Saham (t1) dan Berita (t2) bisa jalan barengan
    # Setelah KEDUANYA selesai, baru masuk Feature Engineering (t3)
    # Terakhir Forecasting (t4)
    [t1, t2] >> t3 >> t4