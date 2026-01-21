import React, { useEffect, useMemo, useRef } from "react";
import * as echarts from "echarts";

import type { DailyKlineItem } from "../services/dailyKlineApi";

interface DailyKlineChartProps {
  items: DailyKlineItem[];
}

const DailyKlineChart: React.FC<DailyKlineChartProps> = ({ items }) => {
  const chartRef = useRef<HTMLDivElement | null>(null);

  const chartData = useMemo(() => {
    const dates: string[] = [];
    const kline: number[][] = [];
    const volume: number[] = [];

    items.forEach((item) => {
      dates.push(item.trade_date);
      kline.push([item.open, item.close, item.low, item.high]);
      volume.push(item.volume);
    });

    return { dates, kline, volume };
  }, [items]);

  useEffect(() => {
    if (!chartRef.current) return;
    const chart = echarts.init(chartRef.current);
    const option = {
      tooltip: { trigger: "axis" },
      axisPointer: { link: [{ xAxisIndex: [0, 1] }] },
      grid: [
        { left: 48, right: 24, top: 40, height: "55%" },
        { left: 48, right: 24, top: "70%", height: "20%" },
      ],
      xAxis: [
        { type: "category", data: chartData.dates, boundaryGap: false },
        { type: "category", data: chartData.dates, gridIndex: 1, boundaryGap: false },
      ],
      yAxis: [{ scale: true }, { gridIndex: 1 }],
      series: [
        { type: "candlestick", data: chartData.kline },
        { type: "bar", xAxisIndex: 1, yAxisIndex: 1, data: chartData.volume },
      ],
    };
    chart.setOption(option);

    const onResize = () => chart.resize();
    window.addEventListener("resize", onResize);

    return () => {
      window.removeEventListener("resize", onResize);
      chart.dispose();
    };
  }, [chartData]);

  if (items.length === 0) {
    return <div>暂无可展示的日线数据。</div>;
  }

  return <div ref={chartRef} style={{ width: "100%", height: 480 }} />;
};

export default DailyKlineChart;
