from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
from pymongo import MongoClient
import os, math
from datetime import datetime

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://stock_prices_mongo:27017/")
DB = os.environ.get("MONGO_DB", "stock_prices")

analyzer = SentimentIntensityAnalyzer()
# load transformer once (costly)
try:
    tf_pipe = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")
except Exception:
    tf_pipe = None

def analyze_text(text):
    res = {}
    vader = analyzer.polarity_scores(text)
    res["vader"] = vader

    transformer = None
    if tf_pipe:
        out = tf_pipe(text[:512])  # truncate
        transformer = out[0]
    res["transformer"] = transformer
    # derive label + confidence
    if transformer:
        label = transformer.get("label")
        conf = transformer.get("score", 0.0)
    else:
        c = vader["compound"]
        if c >= 0.05: label = "POSITIVE"
        elif c <= -0.05: label = "NEGATIVE"
        else: label = "NEUTRAL"
        conf = abs(c)
    return {"label": label, "confidence": float(conf), "raw": res}

def run_sentiment_batch(limit=200):
    client = MongoClient(MONGO_URI)
    db = client[DB]
    raw = db.news_raw
    outc = db.news_sentiment

    cursor = raw.find({"_id": {"$nin": [d["article_id"] for d in outc.find({}, {"article_id":1}).limit(10000)]}}).limit(limit)
    for art in cursor:
        txt = (art.get("title","") or "") + ". " + (art.get("description") or "") + " " + (art.get("content") or "")
        s = analyze_text(txt)
        doc = {
            "article_id": art["_id"],
            "symbol": art.get("symbol"),
            "sentiment": s["raw"],
            "sentiment_label": s["label"].lower(),
            "confidence": s["confidence"],
            "analyzed_at": datetime.utcnow()
        }
        outc.update_one({"article_id": art["_id"]}, {"$set": doc}, upsert=True)
