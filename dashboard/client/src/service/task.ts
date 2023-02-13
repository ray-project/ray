import { StateApiResponse } from "../type/stateApi";
import { Task } from "../type/task";
import { get } from "./requestHandlers";

export const getTasks = (jobId: string | undefined) => {
  let url = "api/v0/tasks?detail=1&limit=10000";
  if (jobId) {
    url += `&job_id=${jobId}`;
  }
  return get<StateApiResponse<Task>>(url);
};

export const downloadTaskTimelineHref = (jobId: string | undefined) => {
  let url = "/api/v0/tasks/timeline?download=1";
  if (jobId) {
    url += `&job_id=${jobId}`;
  }
  return url;
};
