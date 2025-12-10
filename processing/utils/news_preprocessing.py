import pandas as pd
import numpy as np
import re

def clean_text(text: str) -> str:
    """Membersihkan teks berita mentah."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def aggregate_daily_sentiment(df_news: pd.DataFrame) -> pd.DataFrame:
    """Mengubah data berita individual menjadi data sentimen harian."""
    df = df_news.copy()
    
    # Deteksi nama kolom tanggal (bisa 'published_at' atau 'date')
    col_date = 'published_at' if 'published_at' in df.columns else 'date'
    if col_date not in df.columns:
        # Jika tidak ada tanggal, kembalikan dataframe kosong dengan struktur yang benar
        return pd.DataFrame(columns=['trading_date', 'sentiment_score_mean', 
                                   'sentiment_score_std', 'news_volume'])

    df[col_date] = pd.to_datetime(df[col_date])
    
    # Time Alignment: Berita >= 16:00 geser ke besok
    df['trading_date'] = df[col_date].apply(
        lambda x: x.date() + pd.Timedelta(days=1) if x.hour >= 16 else x.date()
    )
    df['trading_date'] = pd.to_datetime(df['trading_date'])

    # Agregasi
    agg_rules = {
        'sentiment_score': ['mean', 'std'], # Hapus min/max biar ringkas, fokus ke mean/std
        'title': 'count' # Volume berita
    }
    
    # Cek apakah kolom title ada, jika tidak gunakan kolom lain untuk hitung volume
    if 'title' not in df.columns:
        df['dummy_count'] = 1
        agg_rules.pop('title')
        agg_rules['dummy_count'] = 'count'

    daily_news = df.groupby('trading_date').agg(agg_rules)
    
    # Flatten MultiIndex Columns
    daily_news.columns = ['_'.join(col).strip() for col in daily_news.columns.values]
    
    # Rename agar standar
    daily_news = daily_news.rename(columns={
        'title_count': 'news_volume',
        'dummy_count_count': 'news_volume'
    })
    
    # Isi NaN pada std (jika cuma 1 berita, std = NaN -> jadi 0)
    daily_news = daily_news.fillna(0)
    
    return daily_news

def merge_news_with_stock(df_stock: pd.DataFrame, df_news_daily: pd.DataFrame) -> pd.DataFrame:
    """Menggabungkan data saham dengan sentimen."""
    
    df_stock['date'] = pd.to_datetime(df_stock['date'])
    
    # Jika df_news_daily kosong/tidak punya trading_date, siapkan struktur dummy
    if df_news_daily.empty or 'trading_date' not in df_news_daily.columns:
        # Kita langsung kembalikan saham dengan kolom sentimen 0
        df_merged = df_stock.copy()
        df_merged['news_volume'] = 0
        df_merged['sentiment_score_mean'] = 0
        df_merged['sentiment_score_std'] = 0
        return df_merged

    # Merge Left
    merged = pd.merge(
        df_stock,
        df_news_daily,
        left_on='date',
        right_on='trading_date',
        how='left'
    )
    
    # --- PERBAIKAN UTAMA (ROBUST FILLNA) ---
    # Kita buat daftar kolom yang wajib ada dan harus diisi 0 jika NaN
    required_cols = ['news_volume', 'sentiment_score_mean', 'sentiment_score_std']
    
    for col in required_cols:
        if col not in merged.columns:
            # Jika kolom tidak ada (misal karena tidak ada berita sama sekali), buat kolomnya
            merged[col] = 0
        else:
            # Jika kolom ada, isi NaN dengan 0
            merged[col] = merged[col].fillna(0)
    
    # Drop kolom bantuan
    if 'trading_date' in merged.columns:
        merged = merged.drop(columns=['trading_date'])
            
    return merged