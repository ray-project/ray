import axios from "axios";
import { EventRsp } from "../type/event";

export const getEvents = (params: any) => {
  return axios.get<EventRsp>(`api/v1/cluster_events?${params}`);
};
