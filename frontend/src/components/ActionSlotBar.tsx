import React from "react";
import { Button, Space, Tooltip } from "antd";

import type { ActionSlotItem } from "../services/actionSlotApi";

interface ActionSlotBarProps {
  items: ActionSlotItem[];
}

const ActionSlotBar: React.FC<ActionSlotBarProps> = ({ items }) => {
  return (
    <Space wrap>
      {items.map((item) => {
        const disabled = item.state !== "enabled";
        const button = (
          <Button key={item.id} disabled={disabled}>
            {item.label}
          </Button>
        );
        return disabled ? (
          <Tooltip key={item.id} title={item.tooltip}>
            {button}
          </Tooltip>
        ) : (
          button
        );
      })}
    </Space>
  );
};

export default ActionSlotBar;
