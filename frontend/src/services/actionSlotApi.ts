import { getJson } from "./httpClient";

export interface ActionSlotItem {
  id: string;
  label: string;
  state: "disabled" | "enabled" | "coming_soon";
  tooltip: string;
}

export interface ActionSlotResponse {
  items: ActionSlotItem[];
}

export function fetchActionSlots(): Promise<ActionSlotResponse> {
  return getJson<ActionSlotResponse>("/ui/action-slots");
}
