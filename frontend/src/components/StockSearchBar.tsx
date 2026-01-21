import React from "react";
import { Input } from "antd";

interface StockSearchBarProps {
  onSearch: (value: string) => void;
  loading?: boolean;
}

const StockSearchBar: React.FC<StockSearchBarProps> = ({ onSearch, loading }) => {
  return (
    <Input.Search
      placeholder="输入股票代码或名称"
      enterButton
      onSearch={onSearch}
      loading={loading}
      style={{ maxWidth: 360 }}
    />
  );
};

export default StockSearchBar;
