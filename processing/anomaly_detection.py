import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest

class StockAnomalyDetector:
    def __init__(self, contamination=0.01):
        """
        contamination: proporsi outlier yang diharapkan dalam dataset (misal 1%)
        """
        self.contamination = contamination
        self.model = IsolationForest(contamination=self.contamination, random_state=42)

    def detect_isolation_forest(self, df: pd.DataFrame, column: str = 'close') -> pd.DataFrame:
        """
        Mendeteksi anomali menggunakan Isolation Forest.
        Menambahkan kolom 'is_anomaly'.
        """
        data = df[[column]].dropna()
        
        # Fit model
        df['anomaly_score'] = self.model.fit_predict(data)
        
        # Isolation Forest return -1 untuk anomali, 1 untuk normal
        # Kita ubah jadi boolean: True jika anomali
        df['is_anomaly'] = df['anomaly_score'].apply(lambda x: x == -1)
        
        # Opsional: Handling anomali (misal replace dengan rolling mean)
        # Hati-hati melakukan ini pada data saham untuk forecasting
        if 'is_anomaly' in df.columns:
             print(f"Deteksi {df['is_anomaly'].sum()} poin data anomali.")
             
        return df.drop(columns=['anomaly_score'])

    def detect_rolling_zscore(self, df: pd.DataFrame, column: str = 'close', window: int = 20, threshold: int = 3) -> pd.DataFrame:
        """
        Mendeteksi anomali berdasarkan deviasi standar dari rolling mean (Bollinger Bands logic).
        """
        roll_mean = df[column].rolling(window=window).mean()
        roll_std = df[column].rolling(window=window).std()
        
        z_score = (df[column] - roll_mean) / roll_std
        df['is_anomaly'] = abs(z_score) > threshold
        
        return df

# Helper function untuk dipanggil dari main pipeline
def run_anomaly_detection(df: pd.DataFrame) -> pd.DataFrame:
    detector = StockAnomalyDetector(contamination=0.02)
    # Menggunakan Isolation Forest sebagai default
    df_processed = detector.detect_isolation_forest(df, column='close')
    return df_processed