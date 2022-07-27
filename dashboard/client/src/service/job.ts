import { JobDetailRsp, JobListRsp } from "../type/job";
import { get } from "./requestHandlers";

export const getJobList = () => {
  return get<JobListRsp>("api/jobs/");
};

export const getJobDetail = (id: string) => {
  return get<JobDetailRsp>(`jobs/${id}`);
};
