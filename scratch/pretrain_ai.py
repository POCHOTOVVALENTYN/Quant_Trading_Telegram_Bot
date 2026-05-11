import pandas as pd
import json
import os
from pathlib import Path

def pretrain_ai_from_csv(csv_path, output_path):
    print(f"--- Загрузка результатов тестов из {csv_path} ---")
    df = pd.read_csv(csv_path)
    
    # Группируем по стратегии и считаем суммарный профит
    # Очистим имена стратегий от префикса Strategy для совпадения с кодом бота
    df['StrategyName'] = df['Стратегия'].str.replace('Strategy', '')
    
    # Агрегируем данные
    strat_stats = df.groupby('StrategyName').agg({
        'Profit USD': 'sum',
        'Win Rate': 'mean',
        'Сделок': 'sum'
    }).reset_index()
    
    # Рассчитываем веса на основе профита (минимум 0.05, чтобы не обнулять стратегии)
    total_profit = strat_stats['Profit USD'].sum()
    strat_stats['Weight'] = strat_stats['Profit USD'] / total_profit
    
    # Приводим к формату FEATURE_KEYS (базовые параметры рынка)
    # Но также сохраним специфические веса для стратегий
    
    weights = {
        "weights": {
            "volume_ratio": 0.20,
            "orderbook_imbalance": 0.15,
            "funding_rate": 0.10,
            "roc_10": 0.15,
            "atr_ratio": 0.10,
            "rsi": 0.10,
            "sma20_to_sma50": 0.10,
            "price_to_sma20": 0.10
        },
        "strategy_performance": strat_stats.set_index('StrategyName')[['Weight', 'Win Rate', 'Profit USD']].to_dict('index'),
        "stats": {
            "last_trained": "2026-05-11T10:30:00Z",
            "num_samples": int(strat_stats['Сделок'].sum()),
            "source": "backtest_csv_pretrain"
        }
    }
    
    # Сохраняем
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(weights, f, indent=4)
    
    print(f"--- Предварительное обучение завершено! ---")
    print(f"Топ-3 стратегии по результатам тестов:")
    for i, row in strat_stats.sort_values('Profit USD', ascending=False).head(3).iterrows():
        print(f"  • {row['StrategyName']}: Профит ${row['Profit USD']:.2f}, WinRate {row['Win Rate']:.1f}%")

if __name__ == "__main__":
    pretrain_ai_from_csv('multi_backtest_results.csv', 'data/learned_weights.json')
