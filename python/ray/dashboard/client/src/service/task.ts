import { StateApiResponse } from "../type/stateApi";
import { Task } from "../type/task";
import { get } from "./requestHandlers";

export const getTasks = (jobId: string | undefined) => {
  let url = "api/v0/tasks?detail=1&limit=10000";
  if (jobId) {
    url += `&filter_keys=job_id&filter_predicates=%3D&filter_values=${jobId}`;
  }
  return get<StateApiResponse<Task>>(url);
};

export const getTask = (taskId: string) => {
  const url = `api/v0/tasks?detail=1&limit=1&filter_keys=task_id&filter_predicates=%3D&filter_values=${encodeURIComponent(
    taskId,
  )}`;
  return get<StateApiResponse<Task>>(url);
};

export const downloadTaskTimelineHref = (jobId: string | undefined) => {
  let url = "api/v0/tasks/timeline?download=1";
  if (jobId) {
    url += `&job_id=${jobId}`;
  }
  return url;
};
