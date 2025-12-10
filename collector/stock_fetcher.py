import os
import pandas as pd
import yfinance as yf
from supabase import create_client, Client
from dotenv import load_dotenv

# Muat variabel environment dari file .env (jika dijalankan lokal)
load_dotenv()

STOCKS_TO_FETCH = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
    "META", "NVDA", "NFLX", "AMD"
]

PERIOD = "1y"
INTERVAL = "1d"

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    # Error handling biar jelas kalau env belum ke-load
    raise Exception("❌ Environment Variables SUPABASE_URL atau SUPABASE_KEY belum disetel!")

# Inisialisasi Client Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_stock_data(ticker):
    """Mengambil data historis dari Yahoo Finance"""
    print(f"📈 Ambil data {ticker}", flush=True)

    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=PERIOD, interval=INTERVAL)

        if df.empty:
            print(f"⚠️ Kosong: {ticker}", flush=True)
            return pd.DataFrame()

        df.reset_index(inplace=True)
        df["ticker"] = ticker
        
        # Pastikan kolom yang diambil benar
        df = df[["Date", "ticker", "Open", "High", "Low", "Close", "Volume"]]
        df.rename(columns={"Date": "datetime"}, inplace=True)

        # Konversi datetime ke format Python native
        df["datetime"] = df["datetime"].dt.to_pydatetime()
        
        print(f"✅ {ticker}: {len(df)} baris ditemukan", flush=True)
        return df

    except Exception as e:
        print(f"❌ Error {ticker}: {e}", flush=True)
        return pd.DataFrame()

def save_to_supabase(df):
    """Menyimpan data ke Supabase dengan metode UPSERT (Update/Insert)"""
    if df.empty:
        return

    records = []
    for r in df.to_dict(orient="records"):
        try:
            records.append({
                # Convert datetime ke string ISO agar aman masuk database
                "datetime": r["datetime"].isoformat() if hasattr(r["datetime"], 'isoformat') else str(r["datetime"]),
                "ticker": r["ticker"],
                "open": float(r["Open"]) if pd.notna(r["Open"]) else 0.0,
                "high": float(r["High"]) if pd.notna(r["High"]) else 0.0,
                "low": float(r["Low"]) if pd.notna(r["Low"]) else 0.0,
                "close": float(r["Close"]) if pd.notna(r["Close"]) else 0.0,
                "volume": int(r["Volume"]) if pd.notna(r["Volume"]) else 0
            })
        except Exception as e:
            print("⚠️ Skip row:", e, flush=True)

    try:
        # PENTING: Gunakan UPSERT, bukan INSERT.
        # Ini mencegah error duplicate key jika script dijalankan berulang kali.
        # Pastikan tabel stock_prices di Supabase punya Primary Key (misal: ticker + datetime)
        supabase.table("stock_prices").upsert(
            records, 
            on_conflict="ticker, datetime" # Kolom unik untuk cek duplikat
        ).execute()
        
        print(f"💾 {len(records)} baris berhasil disimpan/diupdate.", flush=True)
    except Exception as e:
        print(f"❌ Gagal upload ke DB: {e}", flush=True)

def run_once():
    """Fungsi utama yang akan dipanggil oleh Airflow"""
    print("🚀 Stock collector started (Batch Run)...", flush=True)

    for ticker in STOCKS_TO_FETCH:
        df = fetch_stock_data(ticker)
        save_to_supabase(df)
    
    print("🏁 Semua saham selesai diproses.")

# Block ini hanya jalan kalau script di-run manual (python stock_fetcher.py)
# Kalau di-import oleh Airflow, block ini tidak akan jalan otomatis (aman).
if __name__ == "__main__":
    run_once()