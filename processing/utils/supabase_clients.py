import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

# Muat env dari root project
load_dotenv()

def get_supabase_client() -> Client:
    """Membuat koneksi ke Supabase"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL dan SUPABASE_KEY harus disetel di environment variables.")
    return create_client(url, key)

def fetch_data(table_name: str, select_query: str = "*") -> pd.DataFrame:
    """
    Helper untuk mengambil data dari tabel Supabase langsung menjadi Pandas DataFrame.
    """
    client = get_supabase_client()
    try:
        # Mengambil data (limit default supabase biasanya 1000, sesuaikan jika butuh lebih)
        response = client.table(table_name).select(select_query).execute()
        
        data = response.data
        if not data:
            return pd.DataFrame()
            
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error fetching from {table_name}: {e}")
        return pd.DataFrame()

def upload_data(table_name: str, df: pd.DataFrame, if_exists: str = 'append'):
    """
    Helper untuk mengupload DataFrame ke Supabase.
    """
    client = get_supabase_client()
    try:
        # Konversi DataFrame ke format list of dicts (JSON-like)
        data_records = df.to_dict(orient='records')
        
        # Supabase (PostgREST) basic insert
        # Catatan: Supabase insert tidak punya opsi 'replace' bawaan sederhana seperti pandas to_sql.
        # Biasanya kita pakai upsert jika ada primary key.
        client.table(table_name).insert(data_records).execute()
        print(f"Berhasil mengupload {len(data_records)} baris ke {table_name}")
        
    except Exception as e:
        print(f"Error uploading to {table_name}: {e}")