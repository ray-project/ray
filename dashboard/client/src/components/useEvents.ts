import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../common/constants";
import { getEvents } from "../service/event";
export const useEvents = (params: string | null) => {
  return useSWR(
    params ? ["useEvents", params] : null,
    async ([_, params]) => {
      console.info("params: ", params);

      const { data: rspData } = await getEvents(params);
      console.info("data: ", rspData);

      if (rspData?.data?.result) {
        return rspData.data.result;
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );
};
