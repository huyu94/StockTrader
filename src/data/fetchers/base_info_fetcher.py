import os
import pandas as pd
from project_var import DATA_DIR
from src.data.providers.base import BaseProvider
from src.data.providers.tushare_provider import TushareProvider
from src.data.providers.akshare_provider import AkShareProvider

class StockBaseInfoFetcher:
    def __init__(self, provider_name: str = "tushare", provider: BaseProvider | None = None):
        self.stock_basic_info_path = os.path.join(DATA_DIR, "stock_basic_info.csv")
        if provider is not None:
            self.provider: BaseProvider = provider
        else:
            if provider_name == "akshare":
                self.provider: BaseProvider = AkShareProvider()
            else:
                self.provider: BaseProvider = TushareProvider()

    def get_stock_basic_info(self, exchanges: list[str] = None, save_local: bool = True) -> pd.DataFrame:
        df = self.provider.get_stock_basic_info()
        if exchanges and 'exchange' in df.columns:
            df = df[df['exchange'].isin(exchanges)]
        if save_local:
            self.save_stock_basic_info(df)
        return df

    def save_stock_basic_info(self, df: pd.DataFrame):
        if os.path.exists(self.stock_basic_info_path):
            origin_df = pd.read_csv(self.stock_basic_info_path)
            merged_df = pd.concat([origin_df, df]).drop_duplicates(subset=['ts_code'], keep='last')
        else:
            merged_df = df.copy()
        merged_df.to_csv(self.stock_basic_info_path, index=False, encoding='utf-8-sig')

    def get_all_stock_codes(self) -> list:
        if os.path.exists(self.stock_basic_info_path):
            df = pd.read_csv(self.stock_basic_info_path)
            return df['ts_code'].tolist()
        df = self.get_stock_basic_info(save_local=True)
        return df['ts_code'].tolist()
