import { AxiosResponse } from "axios";
import useSWR from "swr";
import { PER_JOB_PAGE_REFRESH_INTERVAL_MS } from "../../../common/constants";
import {
  AsyncFunction,
  StateApiResponse,
  StateApiTypes,
} from "../../../type/stateApi";

export const useStateApiList = (
  key: string,
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
