from src.manager import Manager



def main():
    manager = Manager(provider_name="tushare")
    manager.load_(mode="code", start_date="20250101", end_date="20251222")


if __name__ == "__main__":
    main()