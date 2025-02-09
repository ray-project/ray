import { EventGlobalRsp, EventRsp } from "../type/event";
import { get } from "./requestHandlers";

export const getEvents = (jobId: string) => {
  if (jobId) {
    return get<EventRsp>(`events?job_id=${jobId}`);
  }
};

export const getPipelineEvents = (jobId: string) => {
  if (jobId) {
    return get<EventRsp>(`events?job_id=${jobId}&view=pipeline`);
  }
};

export const getGlobalEvents = () => {
  return get<EventGlobalRsp>("events");
};
