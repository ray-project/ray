import {
  JobListRsp,
  JobProgressByTaskNameRsp,
  JobProgressRsp,
  UnifiedJob,
} from "../type/job";
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

export const getJobProgressByTaskName = (jobId: string) => {
  return get<JobProgressByTaskNameRsp>(
    `api/progress_by_task_name?job_id=${jobId}`,
  );
};
