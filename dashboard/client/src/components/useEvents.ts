import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../common/constants";
import { getEvents } from "../service/event";
import { Filters } from "../type/event";
import { transformFiltersToParams } from "./NewEventTableUtils";

const FIRST_PAGE_NO = 1;
export const useEvents = (filters: Filters | null, pageNo: number) => {
  const params = transformFiltersToParams(filters);

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
