class RiskManager:
    def __init__(self, max_risk_pct: float = 0.02, max_drawdown_pct: float = 0.20, max_open_trades: int = 5):
        """
        Менеджер рисков (Этап 9).
        max risk per trade = 2%
        max drawdown = 20%
        max open trades = 5
        """
        self.max_risk_pct = max_risk_pct
        self.max_drawdown_pct = max_drawdown_pct
        self.max_open_trades = max_open_trades

    def check_trade_allowed(self, current_open_trades: int, current_drawdown_pct: float) -> bool:
        if current_open_trades >= self.max_open_trades:
            return False
            
        if current_drawdown_pct >= self.max_drawdown_pct:
            return False
            
        return True

    def calculate_position_size(self, account_balance: float, entry_price: float, stop_loss_price: float) -> float:
        """
        Position size = Account risk / (Entry - Stop)
        """
        account_risk_usd = account_balance * self.max_risk_pct
        risk_per_coin = abs(entry_price - stop_loss_price)
        
        if risk_per_coin <= 0:
            return 0.0
            
        position_size = account_risk_usd / risk_per_coin
        return position_size

    @staticmethod
    def calculate_atr_stop(entry_price: float, atr: float, signal_type: str = "LONG", multiplier: float = 2.0) -> float:
        """
        Начальный ATR-стоп:
        Stop = Entry - 2 * ATR (LONG)
        Stop = Entry + 2 * ATR (SHORT)
        """
        if signal_type == "LONG":
            return entry_price - (multiplier * atr)
        else:
            return entry_price + (multiplier * atr)

    @staticmethod
    def calculate_trailing_stop(current_stop: float, current_price: float, atr: float, signal_type: str = "LONG", multiplier: float = 2.5) -> float:
        """
        Динамический ATR Trailing Stop (Швагер):
        Для LONG: Стоп только поднимается вверх.
        Stop = Max(OldStop, Price - 2.5 * ATR)
        """
        new_potential_stop = current_price - (multiplier * atr) if signal_type == "LONG" else current_price + (multiplier * atr)
        
        if signal_type == "LONG":
            return max(current_stop, new_potential_stop)
        else:
            # Для SHORT: Стоп только опускается вниз
            return min(current_stop, new_potential_stop)

class TimeExitSystem:
    """15. Time Exit Strategy"""
    @staticmethod
    def should_exit(opened_at_timestamp: float, current_timestamp: float, max_days: int = 5) -> bool:
        """Выход если сделка висит слишком долго без моментума (более 5 дней)"""
        seconds_in_day = 86400
        duration_seconds = current_timestamp - opened_at_timestamp
        if duration_seconds > (max_days * seconds_in_day):
            return True
        return False

class PyramidingSystem:
    def __init__(self):
        """
        Пирамидинг (Этап 11).
        Вход в позицию частями:
        Entry1 = 50%
        Entry2 = 30%
        Entry3 = 20%
        Условие: Price > Entry + 1.5 ATR
        """
        self.allocation = [0.5, 0.3, 0.2]

    def check_next_entry_allowed(self, current_price: float, initial_entry: float, atr: float, signal_type: str = "LONG") -> bool:
        """
        Условие добавления: Price > Entry + 1.5 ATR (для лонгов)
        """
        if signal_type == "LONG":
            return current_price > (initial_entry + 1.5 * atr)
        else:
            return current_price < (initial_entry - 1.5 * atr)
            
    def get_allocation_amount(self, total_size: float, current_stage: int) -> float:
        """
        stage: 0, 1, 2
        """
        if current_stage < len(self.allocation):
            return total_size * self.allocation[current_stage]
        return 0.0
