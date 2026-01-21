import { getJson } from "./httpClient";

export interface StockItem {
  ts_code: string;
  name: string;
  market?: string | null;
}

export interface StockListResponse {
  items: StockItem[];
  total: number;
}

export function fetchStocks(query?: string, limit = 20, offset = 0): Promise<StockListResponse> {
  const params = new URLSearchParams();
  if (query) params.append("query", query);
  params.append("limit", String(limit));
  params.append("offset", String(offset));
  return getJson<StockListResponse>(`/stocks?${params.toString()}`);
}
