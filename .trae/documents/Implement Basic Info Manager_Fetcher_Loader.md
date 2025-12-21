I will implement the `basic_info` management module following the same architectural pattern as the calendar module.

### 1. Create `src/fetchers/basic_info_fetcher.py`
- Create `BasicInfoFetcher` class inheriting structure from `CalendarFetcher`.
- Implement `fetch` method to query `stock_basic` API from Tushare.
- Implement caching logic:
  - Save data to `data/basic_info.csv`.
  - Save update timestamp to `cache/basic_info_update.json`.

### 2. Create `src/loaders/basic_info_loader.py`
- Create `BasicInfoLoader` class inheriting structure from `CalendarLoader`.
- Implement `check_update_needed` method to verify if local data is outdated (daily update).
- Implement `load` method to read from `data/basic_info.csv`.

### 3. Create `src/managers/basic_info_manager.py`
- Create `BasicInfoManager` class.
- Coordinate Fetcher and Loader.
- Provide `get_all_stocks` method that automatically handles updates and returns the stock list.

### 4. Integration & Verification
- Verify the implementation by creating a test script to initialize `BasicInfoManager` and fetch the stock list.
