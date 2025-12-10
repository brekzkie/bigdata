import os
import sys
import time
import traceback
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from supabase import create_client, Client

# --- 1. LOAD ENV (HARDCODE PATH) ---
# Kita gunakan cara yang sama dengan news_fetcher agar pasti berhasil
env_path = r"C:\bigdata-main1\.env"

print(f"🔍 DEBUG: Membaca file kunci di: {env_path}")
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path, override=True)
else:
    print("❌ DEBUG: File .env TIDAK DITEMUKAN!")

# --- 2. SETUP VARIABLE & LIBRARY ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_TABLE = "news_sentiment"  # Pastikan nama tabel benar

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERROR: Kunci Supabase belum diset di .env")
    sys.exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    analyzer = SentimentIntensityAnalyzer() # Inisialisasi otak sentimen
except Exception as e:
    print(f"❌ Gagal inisialisasi: {e}")
    sys.exit(1)

# --- 3. FUNGSI UTAMA ---
def run_sentiment_analysis():
    print("🧠 Sentiment Analyzer started...", flush=True)
    
    # 1. Ambil berita yang sentimennya masih KOSONG (NULL)
    try:
        # Ambil max 50 berita sekaligus biar cepat
        response = supabase.table(SUPABASE_TABLE)\
            .select("*")\
            .is_("sentiment_score", "null")\
            .limit(50)\
            .execute()
        
        articles = response.data
        print(f"   Ditemukan {len(articles)} berita yang perlu dinilai.")
        
    except Exception as e:
        print(f"❌ Gagal ambil data dari DB: {e}")
        return

    # 2. Proses Analisis
    if not articles:
        print("✅ Tidak ada berita baru untuk dianalisis.")
        return

    success_count = 0
    
    for article in articles:
        try:
            # Gabungkan Judul + Deskripsi untuk analisis lebih akurat
            text = f"{article.get('title', '')} {article.get('description', '')}"
            
            # Hitung skor (-1.0 s/d 1.0)
            vs = analyzer.polarity_scores(text)
            compound_score = vs['compound']
            
            # Tentukan Label
            if compound_score >= 0.05:
                label = "POSITIVE"
            elif compound_score <= -0.05:
                label = "NEGATIVE"
            else:
                label = "NEUTRAL"

            # Update ke Database
            supabase.table(SUPABASE_TABLE).update({
                "sentiment_score": compound_score,
                "sentiment_label": label
            }).eq("id", article['id']).execute()
            
            print(f"   ✅ ID {article['id']}: {label} (Score: {compound_score})")
            success_count += 1
            
        except Exception as e:
            print(f"   ⚠️ Gagal update ID {article.get('id')}: {e}")
            continue

    print(f"🏁 Selesai. Berhasil menilai {success_count} berita.")

if __name__ == "__main__":
    run_sentiment_analysis()