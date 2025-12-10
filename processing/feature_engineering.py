import pandas as pd
import numpy as np
from processing.utils.supabase_clients import fetch_data
from processing.utils.news_preprocessing import aggregate_daily_sentiment, merge_news_with_stock

class FeatureEngineer:
    def __init__(self):
        pass

    def add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Pastikan urut waktu agar perhitungan benar
        df = df.sort_values('date')
        
        # 1. Simple Moving Average (SMA)
        df['SMA_5'] = df['close'].rolling(window=5).mean()
        df['SMA_20'] = df['close'].rolling(window=20).mean()
        
        # 2. RSI (Relative Strength Index)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # 3. Volatilitas
        df['volatility'] = df['close'].rolling(window=20).std()
        
        # 4. Daily Return
        df['daily_return'] = df['close'].pct_change()

        return df

    def add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        # Ekstrak fitur waktu
        df['date'] = pd.to_datetime(df['date'])
        df['day_of_week'] = df['date'].dt.dayofweek
        df['month'] = df['date'].dt.month
        return df

    def add_lag_features(self, df: pd.DataFrame, lags=[1, 2, 3]) -> pd.DataFrame:
        # Geser data untuk input model (karena kita prediksi masa depan pakai data masa lalu)
        for lag in lags:
            df[f'close_lag_{lag}'] = df['close'].shift(lag)
        
        # Jika ada data sentimen, buat lag-nya juga
        if 'sentiment_score_mean' in df.columns:
            for lag in lags:
                df[f'sentiment_lag_{lag}'] = df['sentiment_score_mean'].shift(lag)
        return df

def run_feature_engineering_pipeline():
    print("\n=== MEMULAI PIPELINE OTOMATIS (ALL STOCKS) ===")
    
    # 1. AMBIL DATA SAHAM (Load)
    print("1. Mengambil data 'stock_prices'...")
    df_stocks_all = fetch_data("stock_prices")
    
    if df_stocks_all.empty:
        print("!!! ERROR: Tabel stock_prices kosong.")
        return pd.DataFrame()

    # --- PENYESUAIAN KOLOM (Fixing Ticker vs Symbol) ---
    # Di DB Anda namanya 'ticker', di news namanya 'symbol'. Kita samakan jadi 'symbol'.
    if 'ticker' in df_stocks_all.columns:
        print("   > Info: Rename kolom 'ticker' menjadi 'symbol' agar cocok dengan berita.")
        df_stocks_all = df_stocks_all.rename(columns={'ticker': 'symbol'})
    
    # Di DB Anda namanya 'datetime', kita butuh 'date' (hanya tanggal) untuk merge berita
    if 'datetime' in df_stocks_all.columns:
        df_stocks_all['date'] = pd.to_datetime(df_stocks_all['datetime']).dt.normalize()
    elif 'date' not in df_stocks_all.columns:
        print("!!! ERROR: Tidak ada kolom 'datetime' atau 'date'.")
        return pd.DataFrame()

    # 2. AMBIL DATA BERITA
    print("2. Mengambil data 'news_sentiment'...")
    df_news_all = fetch_data("news_sentiment") # Asumsi view/table ini sudah ada
    
    # 3. DETEKSI SAHAM APA SAJA YANG ADA
    unique_symbols = df_stocks_all['symbol'].unique()
    print(f"-> Ditemukan {len(unique_symbols)} saham di DB: {unique_symbols}")

    all_processed_data = []

    # 4. LOOPING: PROSES PER SAHAM (AAPL, MSFT, dll secara otomatis)
    for symbol in unique_symbols:
        print(f"\n--- Memproses: {symbol} ---")
        
        # A. Ambil data saham khusus simbol ini
        df_stock_single = df_stocks_all[df_stocks_all['symbol'] == symbol].copy()
        
        # B. Ambil berita khusus simbol ini (jika ada)
        if not df_news_all.empty and 'symbol' in df_news_all.columns:
            df_news_single = df_news_all[df_news_all['symbol'] == symbol].copy()
        else:
            df_news_single = pd.DataFrame()

        # C. Proses Berita (Agregasi Harian)
        if not df_news_single.empty:
            # Fungsi ini ada di utils/news_preprocessing.py
            df_news_daily = aggregate_daily_sentiment(df_news_single)
        else:
            print(f"   (Info: Tidak ada berita untuk {symbol}, lanjut tanpanya)")
            df_news_daily = pd.DataFrame(columns=['trading_date', 'sentiment_score_mean', 'news_volume'])

        # D. Gabungkan (Merge) Saham + Berita
        # Fungsi ini ada di utils/news_preprocessing.py
        df_merged = merge_news_with_stock(df_stock_single, df_news_daily)

        # E. Hitung Indikator Teknikal
        engineer = FeatureEngineer()
        df_features = engineer.add_technical_indicators(df_merged)
        df_features = engineer.add_time_features(df_features)
        df_final = engineer.add_lag_features(df_features, lags=[1, 2, 3])
        
        # Bersihkan baris NaN (akibat rolling/shift)
        df_final = df_final.dropna().copy()  # Tambahkan .copy()
        
        # Pastikan kolom symbol tetap ada
        df_final['symbol'] = symbol
        
        print(f"   -> Selesai. Data bersih: {len(df_final)} baris.")
        all_processed_data.append(df_final)

    # 5. GABUNGKAN SEMUA HASIL
    if len(all_processed_data) > 0:
        final_df_all = pd.concat(all_processed_data)
        print(f"\n=== SELESAI ===\nTotal Data Siap Training: {final_df_all.shape}")
        # print(final_df_all.head())
        return final_df_all
    else:
        print("Tidak ada data yang berhasil memproses.")
        return pd.DataFrame()

if __name__ == "__main__":
    # Langsung jalankan tanpa parameter, dia akan otomatis cari semua saham
    run_feature_engineering_pipeline()