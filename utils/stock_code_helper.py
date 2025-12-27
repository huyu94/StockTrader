

class StockCodeHelper:
    """
    股票代码辅助类, 用来做ts_code, code 的转换
    """
    @staticmethod
    def validate_stock_code(stock_code: str) -> str:
        """
        验证股票代码格式
        """
        if not stock_code:
            raise ValueError("stock_code cannot be empty")
        if '.' not in stock_code:
            raise ValueError(f"Invalid stock_code format: {stock_code}")
        code, market = stock_code.split('.')
        if len(code) != 6:
            raise ValueError(f"Invalid stock_code format: {stock_code}")
        return stock_code.upper()
