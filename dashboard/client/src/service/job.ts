import { get } from "./requestHandlers";
import { JobDetailRsp, JobListRsp } from "../type/job";

export const getJobList = () => {
  return get<JobListRsp>("jobs?view=summary");
};

export const getJobDetail = (id: string) => {
  return get<JobDetailRsp>(`jobs/${id}`);
};
