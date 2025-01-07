import useSWR from "swr";
import { PER_JOB_PAGE_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getAccelerators } from "../../../service/accelerators";
import { getActors } from "../../../service/actor";
export const useActorList = () => {
  const { data } = useSWR(
    "useActorList",
    async () => {
      const rsp = await getActors();
      const acceleratorsRsp = await getAccelerators();
      if (rsp?.data?.data?.actors) {
        const actors = rsp.data.data.actors;
        const accelerators = acceleratorsRsp.data.data.result;
        return { actors, accelerators };
      } else {
        return {};
      }
    },
    { refreshInterval: PER_JOB_PAGE_REFRESH_INTERVAL_MS },
  );

  return data;
};
