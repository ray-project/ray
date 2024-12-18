import {
  JobListRsp,
  StateApiJobProgressByTaskNameRsp,
  StateApiNestedJobProgressRsp,
  UnifiedJob,
} from "../type/job";
import { get } from "./requestHandlers";

export const getJobList = () => {
  return get<JobListRsp>("api/jobs/");
};

export const getJobDetail = (id: string) => {
  return get<UnifiedJob>(`api/jobs/${id}`);
};

export const getStateApiJobProgressByTaskName = (jobId: string) => {
  return get<StateApiJobProgressByTaskNameRsp>(
    `api/v0/tasks/summarize?filter_keys=job_id&filter_predicates=%3D&filter_values=${jobId}`,
  );
};

export const getStateApiJobProgressByLineage = (jobId: string) => {
  return get<StateApiNestedJobProgressRsp>(
    `api/v0/tasks/summarize?filter_keys=job_id&filter_predicates=%3D&filter_values=${jobId}&summary_by=lineage`,
  );
};
