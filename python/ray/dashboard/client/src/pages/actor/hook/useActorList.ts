import useSWR from "swr";
import { PER_JOB_PAGE_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getActors } from "../../../service/actor";
import { getNodeResourceFlag } from "../../../service/node";

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

export const useNodeResourceFlag = () => {
  const { data } = useSWR(
    "useResourceFlag",
    async () => {
      const rsp = await getNodeResourceFlag();

      if (rsp?.data?.data?.resourceFlag) {
        return rsp.data.data.resourceFlag;
      } else {
        return [];
      }
    },
    { refreshInterval: PER_JOB_PAGE_REFRESH_INTERVAL_MS },
  );

  return data;
};
