import { StateApiResponse } from "../type/stateApi";
import { Task } from "../type/task";
import { get } from "./requestHandlers";

export const getTasks = () => {
  return get<StateApiResponse<Task>>("api/v0/tasks?detail=1&limit=10000");
};
