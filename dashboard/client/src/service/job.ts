import { JobDetailRsp, JobListRsp, JobProgressRsp } from "../type/job";
import { get } from "./requestHandlers";

export const getJobList = () => {
  return get<JobListRsp>("api/jobs/");
};

export const getJobDetail = (id: string) => {
  return get<JobDetailRsp>(`jobs/${id}`);
};

export const getJobProgress = () => {
  return get<JobProgressRsp>("api/progress");
};
