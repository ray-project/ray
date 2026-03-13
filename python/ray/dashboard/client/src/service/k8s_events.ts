import { get } from "./requestHandlers";

export type PlatformEvent = {
  source: string;
  severity: "INFO" | "WARNING" | "ERROR";
  message: string;
  timestampMs: number | string;
  relatedNodeId?: string;
};

export const getK8sEvents = async () => {
  const { data } = await get<{
    result: boolean;
    msg: string;
    data: { events: PlatformEvent[] };
  }>("/api/v0/k8s_events");
  return data.data.events;
};
