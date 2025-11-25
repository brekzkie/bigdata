import os
import time
import pandas as pd
import yfinance as yf
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

# LIST SAHAM
STOCKS_TO_FETCH = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
    "META", "NVDA", "NFLX", "AMD"
]

PERIOD = "1y"
INTERVAL = "1d"

MONGO_HOST = os.environ.get("MONGO_HOST", "cryptodb_mongo")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_DB = os.environ.get("MONGO_DB", "stockdb")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "stock_prices")


def wait_for_mongo():
    print("⏳ Nunggu MongoDB siap...", flush=True)
    while True:
        try:
            client = MongoClient(
                host=MONGO_HOST,
                port=MONGO_PORT,
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
        host=MONGO_HOST,
        port=MONGO_PORT
    )
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]


def fetch_stock_data(ticker):
    print(f"📈 Menarik data saham {ticker} dari Yahoo Finance...", flush=True)

    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=PERIOD, interval=INTERVAL)

        if df.empty:
            print(f"⚠️ Data kosong для {ticker}", flush=True)
            return pd.DataFrame()

        df.reset_index(inplace=True)
        df["ticker"] = ticker

        df = df[["Date", "ticker", "Open", "High", "Low", "Close", "Volume"]]
        df.rename(columns={"Date": "datetime"}, inplace=True)

        # PENTING BIAR MONGO GA NGAMBEK
        df["datetime"] = df["datetime"].dt.to_pydatetime()

        print(f"✅ {ticker} berhasil diambil: {len(df)} baris", flush=True)
        return df

    except Exception as e:
        print(f"❌ Gagal fetch {ticker}: {e}", flush=True)
        return pd.DataFrame()


def save_to_mongo(df):
    if df.empty:
        print("⚠️ Data kosong, skip insert.", flush=True)
        return

    collection = get_collection()
    records = df.to_dict(orient="records")

    if records:
        collection.insert_many(records)
        print(f"💾 {len(records)} data saham masuk MongoDB", flush=True)


if __name__ == "__main__":
    wait_for_mongo()
    print("🚀 AUTO ETL SAHAM AKTIF\n", flush=True)

    total = len(STOCKS_TO_FETCH)

    for i, stock in enumerate(STOCKS_TO_FETCH, 1):
        print(f"\n[{i}/{total}] Memproses saham: {stock}")
        df = fetch_stock_data(stock)
        save_to_mongo(df)

        if i < total:
            print("⏱ Chill 10 detik sebelum lanjut...")
            time.sleep(10)

    print("\n✅ SELESAI! Semua data saham masuk MongoDB.")
