import { StateApiResponse } from "../type/stateApi";
import { Task } from "../type/task";
import { get } from "./requestHandlers";

export const getTasks = () => {
  return get<StateApiResponse<Task>>("api/v0/tasks?detail=1&limit=10000");
};

export const getTaskTimeline = (job_id: string | null) => {
  let url = "api/v0/tasks/timeline";
  if (job_id) {
    url += `?job_id=${job_id}`;
  }
  // Array buffer is required to post data to perfetto.
  return get<string>(url, {
    responseType: "arraybuffer",
  });
};

export const DownloadTaskTimeline = (job_id: string | null) => {
  let url = "api/v0/tasks/timeline";
  if (job_id) {
    url += `?job_id=${job_id}&download=1`;
  }
  // Blob is required to download a file via browser.
  return get<Blob>(url, {
    responseType: "blob",
  });
};
