import axios from "axios";
import { EventGlobalRsp, EventRsp } from "../type/event";

export const getEvents = (jobId: string) => {
  if (jobId) {
    return axios.get<EventRsp>(`events?job_id=${jobId}`);
  }
};

export const getPipelineEvents = (jobId: string) => {
  if (jobId) {
    return axios.get<EventRsp>(`events?job_id=${jobId}&view=pipeline`);
  }
};

export const getGlobalEvents = () => {
  return axios.get<EventGlobalRsp>("events");
};

export const getNewEvents = (params: string) => {
  const baseUrl = "api/v0/cluster_events";
  const apiUrl = new URL(baseUrl);
  apiUrl.search = params;

  return axios.get<EventRsp>(apiUrl.toString());
};
