import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from .feature_engineering import create_features

def detect_regimes(n_clusters: int = 3) -> pd.DataFrame:
    df = create_features()
    feature_cols = ['close', 'volume', 'volatility', 'sentiment_ma_3']
    
    X = df[feature_cols].copy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df['regime'] = kmeans.fit_predict(X_scaled)

    # Beri label
    regime_map = {0: 'Bull', 1: 'Bear', 2: 'Sideways'}
    if n_clusters > 3:
        regime_map = {i: f'Regime_{i}' for i in range(n_clusters)}
    df['regime_label'] = df['regime'].map(regime_map)

    return df

if __name__ == "__main__":
    import os
    os.makedirs("../data/processed", exist_ok=True)
    df = detect_regimes()
    df.to_parquet("../data/processed/regimes.parquet", index=False)
    print("✅ Regime detection selesai.")