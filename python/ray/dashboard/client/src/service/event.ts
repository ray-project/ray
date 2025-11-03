import { EventGlobalRsp, EventRsp } from "../type/event";
import { axiosInstance } from "./requestHandlers";

export const getEvents = (jobId: string) => {
  if (jobId) {
    return axiosInstance.get<EventRsp>(`events?job_id=${jobId}`);
  }
};

export const getPipelineEvents = (jobId: string) => {
  if (jobId) {
    return axiosInstance.get<EventRsp>(`events?job_id=${jobId}&view=pipeline`);
  }
};

export const getGlobalEvents = () => {
  return axiosInstance.get<EventGlobalRsp>("events");
};
