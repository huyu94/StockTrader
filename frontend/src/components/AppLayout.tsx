import React, { useEffect, useState } from "react";
import { Layout, Typography, message } from "antd";

import "antd/dist/reset.css";

import ActionSlotBar from "./ActionSlotBar";
import { fetchActionSlots, ActionSlotItem } from "../services/actionSlotApi";

const { Header, Content } = Layout;

const AppLayout: React.FC<React.PropsWithChildren> = ({ children }) => {
  const [actionSlots, setActionSlots] = useState<ActionSlotItem[]>([]);

  useEffect(() => {
    const loadSlots = async () => {
      try {
        const data = await fetchActionSlots();
        setActionSlots(data.items);
      } catch (error) {
        message.warning("预留按钮加载失败");
      }
    };
    loadSlots();
  }, []);

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Header style={{ display: "flex", alignItems: "center" }}>
        <Typography.Title level={4} style={{ color: "#fff", margin: 0 }}>
          StockTrader Dashboard
        </Typography.Title>
      </Header>
      <Content style={{ padding: "24px" }}>
        {actionSlots.length > 0 && (
          <div style={{ marginBottom: 16 }}>
            <ActionSlotBar items={actionSlots} />
          </div>
        )}
        {children ?? (
          <Typography.Paragraph>
            请选择股票以查看日线行情。
          </Typography.Paragraph>
        )}
      </Content>
    </Layout>
  );
};

export default AppLayout;
