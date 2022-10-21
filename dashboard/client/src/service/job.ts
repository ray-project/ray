import { JobListRsp, JobProgressRsp, UnifiedJob } from "../type/job";
import { get } from "./requestHandlers";

export const getJobList = () => {
  return get<JobListRsp>("api/jobs/");
};

export const getJobDetail = (id: string) => {
  return get<UnifiedJob>(`api/jobs/${id}`);
};

export const getJobProgress = (jobId?: string) => {
  const jobIdQuery = jobId ? `?job_id=${jobId}` : "";
  return get<JobProgressRsp>(`api/progress${jobIdQuery}`);
};
