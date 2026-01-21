import React, { useEffect, useState } from "react";
import { Card, List, Tag } from "antd";

import { fetchDataStatus, DataUpdateStatusItem } from "../services/dataStatusApi";

const statusColor: Record<string, string> = {
  ok: "green",
  warning: "orange",
  error: "red",
};

const DataUpdateStatusPanel: React.FC = () => {
  const [items, setItems] = useState<DataUpdateStatusItem[]>([]);

  useEffect(() => {
    const loadStatus = async () => {
      const data = await fetchDataStatus();
      setItems(data.items);
    };
    loadStatus();
  }, []);

  return (
    <Card title="数据更新状态">
      <List
        dataSource={items}
        renderItem={(item) => (
          <List.Item>
            <div style={{ flex: 1 }}>
              <strong>{item.dataset}</strong>
              {item.coverage_start && item.coverage_end && (
                <span style={{ marginLeft: 8 }}>
                  {item.coverage_start} ~ {item.coverage_end}
                </span>
              )}
            </div>
            <Tag color={statusColor[item.status] || "default"}>
              {item.status}
            </Tag>
          </List.Item>
        )}
      />
    </Card>
  );
};

export default DataUpdateStatusPanel;
