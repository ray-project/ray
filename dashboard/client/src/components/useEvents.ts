import useSWR from "swr";
import { getEvents } from "../service/event";
import { EventRsp, Filters } from "../type/event";
export const useEvents = (params: Filters | null) => {
  //   return useSWR(
  //     params ? ["useActorDetail", params] : null,
  //     async ([_, params]) => {
  //       const event_resp = await getEvents(params);
  //       const { data: rspData } = data;
  //       if (rspData.detail) {
  //         return rspData.detail;
  //       }
  //     },
  //   );
};
