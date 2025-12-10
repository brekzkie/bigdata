import pandas as pd
from .feature_engineering import create_features

def simulate_strategy() -> pd.DataFrame:
    df = create_features().copy()
    df = df.sort_values('date').reset_index(drop=True)

    # Sinyal berdasarkan MA dan sentimen
    df['buy_signal'] = ((df['ma_5'] > df['ma_20']) & (df['sentiment_ma_3'] > 0.1))
    df['sell_signal'] = ((df['ma_5'] < df['ma_20']) | (df['sentiment_ma_3'] < -0.1))

    # Simulasi posisi
    df['position'] = 0
    in_position = False
    for i in range(len(df)):
        if df.loc[i, 'buy_signal'] and not in_position:
            df.loc[i, 'position'] = 1
            in_position = True
        elif df.loc[i, 'sell_signal'] and in_position:
            df.loc[i, 'position'] = -1
            in_position = False

    # Hitung return
    df['daily_return'] = df['close'].pct_change()
    df['strategy_return'] = df['position'].shift(1) * df['daily_return']
    df['cumulative_return'] = (1 + df['strategy_return']).cumprod()

    return df

if __name__ == "__main__":
    import os
    os.makedirs("../data/processed", exist_ok=True)
    df = simulate_strategy()
    df.to_parquet("../data/processed/strategy_results.parquet", index=False)
    total_return = df['cumulative_return'].iloc[-1] - 1
    print(f"✅ Strategi simulasi selesai. Total return: {total_return:.2%}")