import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../common/constants";
import { getEvents } from "../service/event";

const FIRST_PAGE_NO = 1;
export const useEvents = (params: string | null, pageNo: number) => {
  return useSWR(
    params ? ["useEvents", params] : null,
    async ([_, params]) => {
      const { data: rspData } = await getEvents(params);
      if (rspData?.data?.result) {
        return rspData.data.result;
      }
    },
    { refreshInterval: pageNo === FIRST_PAGE_NO ? API_REFRESH_INTERVAL_MS : 0 },
  );
};
