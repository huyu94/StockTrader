from src.manager import Manager



def main():
    manager = Manager(provider_name="tushare")
    df = manager.load_kline_data_from_sql("300642.SZ", "20250101", "20251222")
    print(df)


if __name__ == "__main__":
    main()