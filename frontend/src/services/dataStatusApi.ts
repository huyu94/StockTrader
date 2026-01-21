import { getJson } from "./httpClient";

export interface DataUpdateStatusItem {
  dataset: string;
  last_updated_at: string;
  coverage_start?: string | null;
  coverage_end?: string | null;
  status: string;
  message?: string | null;
}

export interface DataUpdateStatusResponse {
  items: DataUpdateStatusItem[];
}

export function fetchDataStatus(): Promise<DataUpdateStatusResponse> {
  return getJson<DataUpdateStatusResponse>("/status/data-updates");
}
