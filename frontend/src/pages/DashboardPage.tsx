import React, { useState } from "react";
import { Card, Space, message } from "antd";

import DailyKlinePage from "./DailyKlinePage";
import StockList from "../components/StockList";
import StockSearchBar from "../components/StockSearchBar";
import DataUpdateStatusPanel from "../components/DataUpdateStatusPanel";
import { fetchStocks, StockItem } from "../services/stockApi";

const DashboardPage: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [items, setItems] = useState<StockItem[]>([]);
  const [selected, setSelected] = useState<StockItem | null>(null);

  const handleSearch = async (value: string) => {
    setLoading(true);
    try {
      const data = await fetchStocks(value);
      setItems(data.items);
      if (data.items.length === 0) {
        message.info("未找到匹配的股票");
      }
    } catch (error) {
      message.error("股票搜索失败");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <Card>
        <StockSearchBar onSearch={handleSearch} loading={loading} />
      </Card>
      {items.length > 0 && (
        <Card>
          <StockList items={items} onSelect={setSelected} />
        </Card>
      )}
      <DataUpdateStatusPanel />
      <DailyKlinePage initialTsCode={selected?.ts_code} />
    </Space>
  );
};

export default DashboardPage;
