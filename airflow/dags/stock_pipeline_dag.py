import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- KONFIGURASI PATH ---
# Kita perlu menambahkan root folder project ke sys.path agar bisa import 'processing' dan 'collector'
# Asumsi struktur: /opt/airflow/dags/stock_pipeline_dag.py
# Kita mau naik 2 level ke atas untuk mencapai root project
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, '../../'))
sys.path.insert(0, root_dir)

# --- IMPORT MODULE KITA ---
try:
    from collector.stock_fetcher import run_once as fetch_stocks
    # Jika Anda punya news_fetcher, import juga di sini
    from processing.feature_engineering import run_feature_engineering_pipeline
    from processing.forecasting import run_forecasting_pipeline
except ImportError as e:
    print(f"⚠️ Import Error: {e}")
    # Fallback dummy functions biar DAG tidak broken saat di-parse
    def fetch_stocks(): print("Error importing collector")
    def run_feature_engineering_pipeline(): print("Error importing fe")
    def run_forecasting_pipeline(): print("Error importing forecasting")

# --- DEFINISI FUNGSI WRAPPER ---
def task_fetch_data():
    print("--- [TASK 1] Fetching Data Saham ---")
    fetch_stocks()

def task_process_data():
    print("--- [TASK 2] Feature Engineering (News + Technicals) ---")
    run_feature_engineering_pipeline()

def task_train_predict():
    print("--- [TASK 3] Training AI & Forecasting ---")
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
    description='Pipeline Saham End-to-End: Fetch -> Process -> Forecast',
    schedule_interval='0 9 * * *', # Jalan setiap jam 9 pagi
    start_date=datetime(2025, 12, 10),
    catchup=False,
    tags=['bigdata', 'stock', 'ai'],
) as dag:

    # 1. Task Ambil Data
    t1_fetch = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=task_fetch_data,
    )

    # 2. Task Olah Data (Gabung Berita + Indikator)
    t2_process = PythonOperator(
        task_id='feature_engineering',
        python_callable=task_process_data,
    )

    # 3. Task Prediksi (AI)
    t3_forecast = PythonOperator(
        task_id='ai_forecasting',
        python_callable=task_train_predict,
    )

    # --- ATUR URUTAN KERJA (DEPENDENCY) ---
    # Fetch dulu, kalau sukses baru Process, kalau sukses baru Forecast
    t1_fetch >> t2_process >> t3_forecast