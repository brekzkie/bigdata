import pandas as pd

def build_features(df):
    df['return'] = df['close'].pct_change()
    df['ma_5'] = df['close'].rolling(5).mean()
    df['ma_20'] = df['close'].rolling(20).mean()
    df['volatility'] = df['return'].rolling(10).std()
    return df.dropna()
