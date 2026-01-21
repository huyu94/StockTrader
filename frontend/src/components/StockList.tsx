import React from "react";
import { List, Typography } from "antd";

import type { StockItem } from "../services/stockApi";

interface StockListProps {
  items: StockItem[];
  onSelect: (stock: StockItem) => void;
}

const StockList: React.FC<StockListProps> = ({ items, onSelect }) => {
  return (
    <List
      bordered
      dataSource={items}
      renderItem={(item) => (
        <List.Item onClick={() => onSelect(item)} style={{ cursor: "pointer" }}>
          <Typography.Text>{item.ts_code}</Typography.Text>
          <Typography.Text type="secondary" style={{ marginLeft: 8 }}>
            {item.name}
          </Typography.Text>
        </List.Item>
      )}
    />
  );
};

export default StockList;
