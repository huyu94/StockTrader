import { getJson } from "./httpClient";

export interface DailyKlineItem {
  trade_date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  amount?: number | null;
  adj_factor?: number | null;
}

export interface DailyKlineResponse {
  ts_code: string;
  items: DailyKlineItem[];
}

export function fetchDailyKlines(
  tsCode: string,
  startDate?: string,
  endDate?: string,
): Promise<DailyKlineResponse> {
  const params = new URLSearchParams();
  if (startDate) params.append("start_date", startDate);
  if (endDate) params.append("end_date", endDate);
  const query = params.toString();
  const path = `/stocks/${tsCode}/daily${query ? `?${query}` : ""}`;
  return getJson<DailyKlineResponse>(path);
}
