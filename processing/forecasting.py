import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error

# Import pipeline feature engineering
from processing.feature_engineering import run_feature_engineering_pipeline
# Import utils untuk upload hasil
from processing.utils.supabase_clients import upload_data

class StockForecaster:
    def __init__(self, model_params=None):
        if model_params is None:
            self.model = RandomForestRegressor(
                n_estimators=100, 
                max_depth=15, 
                random_state=42
            )
        else:
            self.model = RandomForestRegressor(**model_params)

    def prepare_data(self, df_single_stock, target_col='close', test_size=0.2):
        """Memisahkan data Train dan Test."""
        
        # 1. Daftar kolom yang HARUS DIBUANG (bukan fitur prediksi)
        # SAYA TAMBAHKAN 'id' DI SINI AGAR TIDAK ERROR
        drop_cols = [
            'date', 'symbol', 'ticker', 
            'close', 'adjusted_close', # Close dibuang dari X karena dia adalah target (y)
            'datetime', 'created_at', 'id', 'published_at'
        ]
        
        # Buang kolom yang ada di daftar drop_cols (errors='ignore' biar gak error kalau kolomnya gak ada)
        X = df_single_stock.drop(columns=drop_cols, errors='ignore')
        
        # 2. FILTER EXTRA (PENTING): Pastikan HANYA ANGKA yang tersisa
        # Ini akan membuang kolom string lain yang mungkin terselip
        X = X.select_dtypes(include=[np.number])
        
        y = df_single_stock[target_col]

        # Time Series Split
        split_idx = int(len(df_single_stock) * (1 - test_size))
        
        X_train = X.iloc[:split_idx]
        X_test = X.iloc[split_idx:]
        y_train = y.iloc[:split_idx]
        y_test = y.iloc[split_idx:]
        
        # Simpan nama fitur untuk keperluan debug/analisis
        feature_names = X.columns.tolist()
        
        return X_train, X_test, y_train, y_test, feature_names

    def train(self, X_train, y_train):
        self.model.fit(X_train, y_train)

    def evaluate(self, X_test, y_test):
        predictions = self.model.predict(X_test)
        mae = mean_absolute_error(y_test, predictions)
        return predictions, mae

    def predict_future(self, last_row_features):
        """Memprediksi harga besok."""
        return self.model.predict(last_row_features)[0]

def run_forecasting_pipeline():
    print("\n=== MEMULAI FORECASTING AI ===")
    
    # 1. Ambil Data Matang
    # Karena feature engineering sudah jalan sukses, ini akan cepat
    df_all_stocks = run_feature_engineering_pipeline()
    
    if df_all_stocks.empty:
        print("Data kosong. Tidak bisa lanjut.")
        return

    all_results = []
    unique_symbols = df_all_stocks['symbol'].unique()
    
    # 2. Loop per Saham
    for symbol in unique_symbols:
        print(f"\n📈 Training Model untuk: {symbol}")
        
        # Filter per saham & Urutkan waktu
        df_single = df_all_stocks[df_all_stocks['symbol'] == symbol].copy()
        df_single = df_single.sort_values('date')
        
        forecaster = StockForecaster()
        
        # Split Data
        X_train, X_test, y_train, y_test, feature_names = forecaster.prepare_data(df_single)
        
        # Cek keamanan data sebelum train
        if X_train.empty:
            print(f"   (Skip: Data {symbol} terlalu sedikit untuk training)")
            continue
            
        # Train
        forecaster.train(X_train, y_train)
        
        # Evaluate
        y_pred_test, mae = forecaster.evaluate(X_test, y_test)
        print(f"   > Error Rata-rata (MAE): ${mae:.2f}")
        
        # Predict Next Day
        last_row = X_test.iloc[[-1]] 
        next_price = forecaster.predict_future(last_row)
        last_date = df_single['date'].iloc[-1]
        
        # Print hasil agar terlihat di terminal
        last_price_val = y_test.iloc[-1]
        print(f"   > Harga Terakhir ({last_date.date()}): ${last_price_val:.2f}")
        print(f"   > PREDIKSI BESOK: ${next_price:.2f}")
        
        # Sinyal Sederhana (Naik/Turun)
        signal = "BUY" if next_price > last_price_val else "SELL"
        print(f"   > Sinyal: {signal}")

        # Simpan Hasil
        result_row = {
            'symbol': symbol,
            'last_date': str(last_date.date()),
            'last_price': float(last_price_val),
            'predicted_price': float(next_price),
            'mae_error': float(mae),
            'signal': signal,
            'created_at': pd.Timestamp.now().isoformat()
        }
        all_results.append(result_row)

    # 3. Upload Hasil
    if all_results:
        df_results = pd.DataFrame(all_results)
        print("\n📊 Rekapitulasi Prediksi:")
        print(df_results[['symbol', 'last_price', 'predicted_price', 'signal']])
        
        print("\nMencoba upload ke tabel 'forecast_results' di Supabase...")
        # Note: Ini mungkin gagal jika tabel belum dibuat di Supabase, tapi tidak bikin crash
        try:
            upload_data("forecast_results", df_results)
        except Exception as e:
            print(f"Info: Gagal upload (Mungkin tabel belum ada). Error: {e}")
            print("Tapi hasil prediksi sudah muncul di layar ^")
    
    print("\n=== SELESAI ===")

if __name__ == "__main__":
    run_forecasting_pipeline()