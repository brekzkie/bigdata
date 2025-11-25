import requests
import os
import time
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VS_CURRENCY = "usd"
DAYS = "365"

# LIST GABUNGAN (~40 Koin Terpopuler)
COINS_TO_FETCH = [
    "bitcoin", "ethereum", "tether", "binancecoin", "solana",
    "ripple", "usd-coin", "staked-ether", "cardano", "avalanche-2",
    "tron", "chainlink", "polkadot", "matic-network", "litecoin",
    "bitcoin-cash", "uniswap", "near", "cosmos", "algorand",
    "aptos", "sui", "hedera-hashgraph", "tezos", "eos", "fantom",
    "dogecoin", "shiba-inu", "pepe", "floki",
    "bonk", "dogwifhat", "memecoin",
    "render-token", "fetch-ai", "singularitynet",
    "the-sandbox", "decentraland", "axie-infinity",
    "gala", "immutable-x"
]

# Session + Retry
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

def fetch_data(coin):
    url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
    print(f"📥 Menarik data {coin} dari CoinGecko...", flush=True)

    try:
        response = session.get(
            url,
            params={'vs_currency': VS_CURRENCY, 'days': DAYS},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
            verify=False
        )
        response.raise_for_status()
        data = response.json()
        prices = data.get('prices', [])

        if not prices:
            print("⚠️ API kosong, no data.", flush=True)
            return pd.DataFrame()

        df = pd.DataFrame(prices, columns=['timestamp', 'price'])
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['coin_id'] = coin
        df = df[['datetime', 'coin_id', 'price']]
        print(f"✅ Data berhasil diambil: {len(df)} baris", flush=True)
        return df

    except Exception as e:
        print(f"❌ Gagal fetch data {coin}: {e}", flush=True)
        return pd.DataFrame()

def wait_for_mongo():
    print("⏳ Nunggu MongoDB siap...", flush=True)
    while True:
        try:
            client = MongoClient(
                host=os.environ.get("MONGO_HOST", "mongodb"),
                port=int(os.environ.get("MONGO_PORT", 27017)),
                serverSelectionTimeoutMS=3000
            )
            client.server_info()
            print("✅ MongoDB ready!", flush=True)
            return
        except ServerSelectionTimeoutError:
            print("...Mongo belum siap, retry 3 detik...", flush=True)
            time.sleep(3)

def get_collection():
    client = MongoClient(
        host=os.environ.get("MONGO_HOST", "mongodb"),
        port=int(os.environ.get("MONGO_PORT", 27017))
    )
    db = client[os.environ.get("MONGO_DB", "cryptodb")]
    return db[os.environ.get("MONGO_COLLECTION", "coin_prices")]

def save_to_mongo(df):
    if df.empty:
        print("⚠️ Data kosong, skip insert.", flush=True)
        return

    collection = get_collection()
    for record in df.to_dict(orient="records"):
        collection.update_one(
            {"datetime": record["datetime"], "coin_id": record["coin_id"]},
            {"$set": record},
            upsert=True
        )
    print("💾 Data berhasil disimpan ke MongoDB", flush=True)

if __name__ == "__main__":
    wait_for_mongo()
    print("🚀 AUTO ETL AKTIF\n", flush=True)

    total_coins = len(COINS_TO_FETCH)
    for i, coin in enumerate(COINS_TO_FETCH, 1):
        print(f"\n[{i}/{total_coins}] Memproses: {coin}")
        df = fetch_data(coin)
        save_to_mongo(df)

        if i < total_coins:
            print("⏱ Tunggu 15 detik sebelum koin berikutnya...")
            time.sleep(15)

    print("\n✅ SELESAI! Semua data koin telah ditarik dan disimpan.")
