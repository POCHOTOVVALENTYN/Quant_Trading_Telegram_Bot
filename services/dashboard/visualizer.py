import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class DashboardVisualizer:
    """
    Дашборд-визуализатор для исторических сделок (Этап 17).
    Использует Plotly для расчета и отрисовки статистики.
    """
    
    def __init__(self, initial_balance: float = 10000.0):
        self.initial_balance = initial_balance

    def calculate_metrics(self, df_trades: pd.DataFrame) -> dict:
        """
        Рассчитывает ключевые показатели на основе списка закрытых сделок.
        Ожидаются столбцы в df: ['timestamp', 'pnl'] (pnl = Profit and Loss в $)
        """
        if df_trades.empty:
            return {
                "balance": self.initial_balance,
                "win_rate": 0.0,
                "avg_rr": 0.0,
                "max_drawdown": 0.0,
                "profit_factor": 0.0
            }

        # Накопительная кривая доходности (Equity curve)
        df_trades['equity'] = self.initial_balance + df_trades['pnl'].cumsum()
        
        # Win rate (Винрейт)
        wins = df_trades[df_trades['pnl'] > 0]
        losses = df_trades[df_trades['pnl'] < 0]
        win_rate = len(wins) / len(df_trades) * 100 if len(df_trades) > 0 else 0.0
        
        # Average RR (Среднее соотношение риск/прибыль)
        avg_win = wins['pnl'].mean() if len(wins) > 0 else 0.0
        avg_loss = abs(losses['pnl'].mean()) if len(losses) > 0 else 1.0 # Защита от деления на 0
        avg_rr = avg_win / avg_loss if avg_loss != 0 else 0.0

        # Максимальная просадка (Max drawdown)
        df_trades['max_equity_so_far'] = df_trades['equity'].cummax()
        df_trades['drawdown'] = (df_trades['max_equity_so_far'] - df_trades['equity']) / df_trades['max_equity_so_far']
        max_drawdown = df_trades['drawdown'].max() * 100 # В процентах

        # Профит фактор (Profit factor)
        gross_profit = wins['pnl'].sum()
        gross_loss = abs(losses['pnl'].sum())
        profit_factor = gross_profit / gross_loss if gross_loss != 0 else float('inf')

        return {
            "balance": df_trades.iloc[-1]['equity'] if not df_trades.empty else self.initial_balance,
            "win_rate": round(win_rate, 2),
            "avg_rr": round(avg_rr, 2),
            "max_drawdown": round(max_drawdown, 2),
            "profit_factor": round(profit_factor, 2),
            "df_processed": df_trades
        }

    def generate_html_report(self, df_trades: pd.DataFrame) -> str:
        """
        Генерирует HTML верстку дашборда с графиками Plotly.
        """
        metrics = self.calculate_metrics(df_trades)
        
        if df_trades.empty:
            return "<html><body><h2>Нет данных о сделках для визуализации.</h2></body></html>"
            
        df = metrics["df_processed"]

        # Создаем фигуру Plotly (Subplots: сверху кривая доходности, снизу просадка)
        fig = make_subplots(
            rows=2, cols=1, 
            shared_xaxes=True, 
            vertical_spacing=0.1,
            subplot_titles=('Кривая доходности (Equity curve)', 'Просадка (Drawdown %)')
        )

        # 1. Кривая доходности
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'], 
                y=df['equity'], 
                mode='lines+markers',
                name='Баланс',
                line=dict(color='blue')
            ), 
            row=1, col=1
        )

        # 2. Просадка (Красная зона под графиком)
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'], 
                y=df['drawdown'] * -100, # Отрицательное значение для визуальности
                fill='tozeroy',
                mode='lines',
                name='Просадка',
                line=dict(color='red')
            ), 
            row=2, col=1
        )

        # Настраиваем макет
        fig.update_layout(
            title_text='Аналитика Торговой Стратегии (Dashboard)',
            height=800,
            showlegend=False,
            template="plotly_white"
        )
        
        # Генерируем HTML код самого графика
        graph_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

        # Формируем красивую HTML-страничку с метриками (перевод на русский по ТЗ)
        html_template = f"""
        <html>
        <head>
            <title>algo-bot dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f9f9f9; }}
                .stats-container {{ display: flex; justify-content: space-around; margin-bottom: 30px; }}
                .stat-box {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); text-align: center; width: 18%; }}
                h3 {{ color: #555; margin-bottom: 5px; font-size: 14px; text-transform: uppercase; }}
                p {{ font-size: 24px; font-weight: bold; margin: 0; color: #111; }}
            </style>
        </head>
        <body>
            <h1>📊 Отчет алгоритмической торговли</h1>
            
            <div class="stats-container">
                <div class="stat-box">
                    <h3>Текущий баланс</h3>
                    <p>${metrics['balance']:,.2f}</p>
                </div>
                <div class="stat-box">
                    <h3>Винрейт (Win Rate)</h3>
                    <p>{metrics['win_rate']}%</p>
                </div>
                <div class="stat-box">
                    <h3>Средний риск/прибыль (Avg RR)</h3>
                    <p>{metrics['avg_rr']}</p>
                </div>
                <div class="stat-box">
                    <h3>Максимальная просадка</h3>
                    <p>{metrics['max_drawdown']}%</p>
                </div>
                <div class="stat-box">
                    <h3>Профит фактор</h3>
                    <p>{metrics['profit_factor']}</p>
                </div>
            </div>

            <div style="background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                {graph_html}
            </div>
            
        </body>
        </html>
        """
        
        return html_template

# Моковые данные для тестирования (чтобы Дашборд можно было запустить сразу)
if __name__ == "__main__":
    import datetime
    
    # Симуляция 10 закрытых сделок
    mock_trades = [
        {"timestamp": datetime.datetime(2023, 1, 1), "pnl": 150},
        {"timestamp": datetime.datetime(2023, 1, 2), "pnl": -50},
        {"timestamp": datetime.datetime(2023, 1, 3), "pnl": 200},
        {"timestamp": datetime.datetime(2023, 1, 4), "pnl": -100},
        {"timestamp": datetime.datetime(2023, 1, 5), "pnl": -80},
        {"timestamp": datetime.datetime(2023, 1, 6), "pnl": 500},
        {"timestamp": datetime.datetime(2023, 1, 7), "pnl": -120},
        {"timestamp": datetime.datetime(2023, 1, 8), "pnl": 300},
        {"timestamp": datetime.datetime(2023, 1, 9), "pnl": 150},
        {"timestamp": datetime.datetime(2023, 1, 10), "pnl": 400},
    ]
    df_test = pd.DataFrame(mock_trades)
    
    viz = DashboardVisualizer()
    html_report = viz.generate_html_report(df_test)
    
    with open("dashboard_test.html", "w", encoding="utf-8") as f:
        f.write(html_report)
        
    print("Тестовый дашборд сгенерирован в 'dashboard_test.html'")
