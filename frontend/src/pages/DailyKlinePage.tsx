import React, { useEffect, useState } from "react";
import { Button, Card, Input, Space, Typography, message } from "antd";

import DailyKlineChart from "../components/DailyKlineChart";
import { fetchDailyKlines, DailyKlineItem } from "../services/dailyKlineApi";

interface DailyKlinePageProps {
  initialTsCode?: string;
}

const DailyKlinePage: React.FC<DailyKlinePageProps> = ({ initialTsCode }) => {
  const [tsCode, setTsCode] = useState(initialTsCode ?? "");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [items, setItems] = useState<DailyKlineItem[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (initialTsCode) {
      setTsCode(initialTsCode);
    }
  }, [initialTsCode]);

  const handleLoad = async () => {
    if (!tsCode) {
      message.warning("请输入股票代码");
      return;
    }
    setLoading(true);
    try {
      const data = await fetchDailyKlines(tsCode, startDate || undefined, endDate || undefined);
      setItems(data.items);
    } catch (error) {
      message.error("加载日线数据失败");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <Card>
        <Space wrap>
          <Input
            placeholder="股票代码，例如 000001.SZ"
            value={tsCode}
            onChange={(event) => setTsCode(event.target.value)}
            style={{ width: 200 }}
          />
          <Input
            placeholder="起始日期 YYYYMMDD"
            value={startDate}
            onChange={(event) => setStartDate(event.target.value)}
            style={{ width: 160 }}
          />
          <Input
            placeholder="结束日期 YYYYMMDD"
            value={endDate}
            onChange={(event) => setEndDate(event.target.value)}
            style={{ width: 160 }}
          />
          <Button type="primary" onClick={handleLoad} loading={loading}>
            加载日线
          </Button>
        </Space>
      </Card>
      <Card>
        {items.length === 0 ? (
          <Typography.Paragraph>请选择股票并加载数据。</Typography.Paragraph>
        ) : (
          <DailyKlineChart items={items} />
        )}
      </Card>
    </Space>
  );
};

export default DailyKlinePage;
