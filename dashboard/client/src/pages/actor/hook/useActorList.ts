import useSWR from "swr";
import { PER_JOB_PAGE_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getActors } from "../../../service/actor";

export const useActorList = () => {
  const { data } = useSWR(
    "useActorList",
    async () => {
      const rsp = await getActors();

      if (rsp?.data?.data?.actors) {
        return rsp.data.data.actors;
      } else {
        return {};
      }
    },
    { refreshInterval: PER_JOB_PAGE_REFRESH_INTERVAL_MS },
  );

  return data;
};
