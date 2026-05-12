
class SymbolNormalizer:
    """
    Сервис для унификации работы с торговыми парами (символами).
    Обеспечивает единый формат внутри системы, для API и БД.
    """

    @staticmethod
    def normalize(s: str) -> str:
        """
        Преобразует строку в единый внутренний формат: BTC/USDT.
        """
        if not s:
            return ""
        # Удаляем лишнее (интервалы, слэши, суффиксы Binance)
        clean = s.split(":")[0].replace("/", "").strip().upper()
        
        if clean.endswith("USDT") and len(clean) > 4:
            return f"{clean[:-4]}/USDT"
        
        # Если формат уже содержит слэш (напр. BTC/USDT), возвращаем как есть (после upper)
        if "/" in s:
            parts = s.split("/")
            return f"{parts[0].strip().upper()}/{parts[1].strip().upper()}"
            
        return clean

    @staticmethod
    def to_binance(symbol: str) -> str:
        if not symbol: return ""
        # Удаляем / и - и приводим к верхнему регистру
        s = symbol.upper().replace("/", "").replace("-", "")
        return s

    @staticmethod
    def to_db(s: str) -> str:
        """
        Преобразует в формат для хранения в БД (сейчас совпадает с нормализованным).
        """
        return SymbolNormalizer.normalize(s)
