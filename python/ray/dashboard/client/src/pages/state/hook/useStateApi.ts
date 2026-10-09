import { AxiosResponse } from "axios";
import useSWR, { Key } from "swr";
import { PER_JOB_PAGE_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getTask } from "../../../service/task";
import {
  AsyncFunction,
  StateApiResponse,
  StateApiTypes,
} from "../../../type/stateApi";

export const useStateApiList = (
  key: Key,
  getFunc: AsyncFunction<AxiosResponse<StateApiResponse<StateApiTypes>>>,
) => {
  /**
   * getFunc is a method defined within ../service.
   */
  const { data } = useSWR(
    key,
    async () => {
      const rsp = await getFunc();
      if (rsp?.data?.data?.result?.result) {
        return rsp.data.data.result.result;
      } else {
        return [];
      }
    },
    { refreshInterval: PER_JOB_PAGE_REFRESH_INTERVAL_MS },
  );

  return data;
};

export const useStateApiTask = (taskId: string | undefined) => {
  const { data, isLoading } = useSWR(
    taskId ? ["useStateApiTask", taskId] : null,
    async ([_, taskId]) => {
      const rsp = await getTask(taskId);
      if (rsp?.data?.data?.result?.result) {
        return rsp.data.data.result.result[0];
      } else {
        return undefined;
      }
    },
    { refreshInterval: PER_JOB_PAGE_REFRESH_INTERVAL_MS },
  );

  return {
    task: data,
    isLoading,
  };
};
