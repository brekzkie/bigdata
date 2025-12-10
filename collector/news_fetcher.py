import os
import sys
import traceback
import requests
import urllib3
import time  # <--- 1. IMPORT TIME
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# --- BAGIAN 1: LOAD ENV ---
env_path = r"C:\bigdata-main1\.env"
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path, override=True)

# --- BAGIAN 2: SETUP ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NEWS_API_KEY = os.environ.get("NEWS_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_TABLE = "news_sentiment"

# Cek Kunci
if not all([NEWS_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
    print("❌ ERROR: Kunci di .env masih kosong!")
    sys.exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception:
    print("❌ Gagal konek ke Supabase")
    sys.exit(1)

# --- BAGIAN 3: FUNGSI AMBIL BERITA ---
def fetch_cnbc(query="stock", page=1):
    try:
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query,
            "sortBy": "publishedAt",
            "apiKey": NEWS_API_KEY,
            "pageSize": 10, # <-- TIPS: Kurangi jadi 10 biar hemat
            "page": page,
            "language": "en"
        }
        
        # verify=False SOLUSI UTAMA error SSL/koneksi
        r = requests.get(url, params=params, timeout=20, verify=False)
        
        if r.status_code == 429:
             print("❌ Error 429: KUOTA HABIS! Ganti API Key baru.")
             return []
        if r.status_code == 401:
            print("❌ Error 401: API Key Salah!")
            return []
            
        r.raise_for_status()
        return r.json().get("articles", [])

    except Exception as e:
        print(f"❌ Gagal fetch {query}: {e}")
        return []

# --- BAGIAN 4: LOGIKA UTAMA ---
def run_fetch():
    print("\n📰 News collector started...", flush=True)
    targets = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    
    for q in targets:
        print(f"🔎 Fetching news for {q}...", flush=True)
        arts = fetch_cnbc(q)

        supabase_rows = []
        for a in arts:
            try:
                published = a.get("publishedAt")
                if not published: continue
                published_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))

                supabase_rows.append({
                    "symbol": q,
                    "title": a.get("title"),
                    "description": a.get("description"),
                    "content": a.get("content"),
                    "published_at": published_dt.isoformat(),
                    "source": a.get("source", {}).get("name"),
                    "url": a.get("url")
                })
            except Exception:
                continue

        if supabase_rows:
            try:
                supabase.table(SUPABASE_TABLE).upsert(
                    supabase_rows, on_conflict="url", ignore_duplicates=True
                ).execute()
                print(f"   ✅ Saved: {len(supabase_rows)} items.")
            except Exception as e:
                print(f"   ❌ DB Error: {e}")
        else:
            print("   ⚠️ No news found.")
        
        # --- 2. TAMBAHKAN JEDA BIAR GAK KENA RATE LIMIT ---
        print("   ⏳ Istirahat 5 detik...", flush=True)
        time.sleep(5) 

    print("🏁 Done.")

if __name__ == "__main__":
    run_fetch()