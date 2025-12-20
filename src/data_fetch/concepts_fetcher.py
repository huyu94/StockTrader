import akshare as ak
from loguru import logger

def _to_ts_code(code: str) -> str:
    code = str(code)
    if code.startswith('6'):
        return f"{code}.SH"
    elif code.startswith('8') or code.startswith('4'):
        return f"{code}.BJ"
    else:
        return f"{code}.SZ"

class ConceptsFetcher:
    def __init__(self):
        logger.info("ConceptsFetcher initialized (AkShare)")

    def get_all_concepts(self):
        try:
            df = ak.stock_board_concept_name_ths()
            return df['概念名称'].tolist()
        except Exception as e:
            logger.error(f"获取概念列表失败：{e}")
            return []

    def get_stocks_by_concept(self, concept_name: str):
        try:
            df = ak.stock_board_concept_cons_ths(symbol=concept_name)
            if df is None or df.empty:
                return []
            return [_to_ts_code(code) for code in df['代码'].tolist()]
        except Exception as e:
            logger.error(f"获取概念 {concept_name} 成分股失败：{e}")
            return []

