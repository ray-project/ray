import { useContext, useState } from "react";
import { useParams } from "react-router-dom";
import useSWR from "swr";
import { GlobalContext } from "../../../App";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { ActorResp, getActor } from "../../../service/actor";

export const useActorDetail = () => {
  const params = useParams() as { id: string };
  const [msg, setMsg] = useState("Loading the actor infos...");
  const { namespaceMap } = useContext(GlobalContext);

  const { data: actorDetail } = useSWR(
    ["useActorDetail", params.id],
    async (_, actorId) => {
      const actor_resp = await getActor(actorId);
      const data: ActorResp = actor_resp?.data;
      const { data: rspData, msg, result } = data;
      if (msg) {
        setMsg(msg);
      }

      if (result === false) {
        setMsg("Actor Query Error Please Check Actor Id");
      }

      if (rspData.detail) {
        return rspData.detail;
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  return {
    params,
    actorDetail,
    msg,
    namespaceMap,
  };
};
