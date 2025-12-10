import requests, os
from datetime import datetime
from supabase import create_client, Client
import traceback
import sys

# =========================
# ENV
# =========================
NEWS_API_KEY = os.environ.get("NEWS_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_TABLE = os.environ.get("SUPABASE_NEWS_TABLE", "news_raw")

# =========================
# Cek ENV
# =========================
missing_env = []
for env_name, env_val in [("SUPABASE_URL", SUPABASE_URL), 
                          ("SUPABASE_KEY", SUPABASE_KEY), 
                          ("NEWS_API_KEY", NEWS_API_KEY)]:
    if not env_val:
        missing_env.append(env_name)

if missing_env:
    print(f"❌ ENV belum diset: {', '.join(missing_env)}")
    sys.exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception:
    print("❌ Gagal konek ke Supabase")
    traceback.print_exc()
    sys.exit(1)

# =========================
# FETCHER
# =========================
def fetch_cnbc(query="stock", page=1):
    try:
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query,
            "sortBy": "publishedAt",
            "apiKey": NEWS_API_KEY,
            "pageSize": 50,
            "page": page
        }
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json().get("articles", [])
    except Exception:
        print(f"❌ Gagal fetch berita untuk query {query}")
        traceback.print_exc()
        return []

# =========================
# CORE
# =========================
def run_fetch():
    print("📰 News collector started...", flush=True)

    for q in ["AAPL", "MSFT", "TSLA"]:
        print(f"🔎 Fetching news for {q}", flush=True)
        arts = fetch_cnbc(q)

        supabase_rows = []

        for a in arts:
            try:
                published = a.get("publishedAt")
                if not published:
                    continue
                published_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))

                supabase_rows.append({
                    "title": a.get("title"),
                    "description": a.get("description"),
                    "content": a.get("content"),
                    "symbol": q,
                    "published_at": published_dt.isoformat(),
                    "source": a.get("source", {}).get("name"),
                    "url": a.get("url")
                })
            except Exception:
                print(f"❌ Error proses artikel: {a.get('title')}")
                traceback.print_exc()
                continue

        # Save to Supabase
        if supabase_rows:
            try:
                supabase.table(SUPABASE_TABLE).upsert(supabase_rows).execute()
                print(f"✅ {len(supabase_rows)} news inserted to Supabase", flush=True)
            except Exception:
                print("❌ Supabase insert failed")
                traceback.print_exc()

    print("✅ News fetch done", flush=True)

if __name__ == "__main__":
    try:
        run_fetch()
    except Exception:
        print("❌ Script crash utama")
        traceback.print_exc()
