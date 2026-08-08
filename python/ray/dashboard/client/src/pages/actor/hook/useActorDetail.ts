import axios from "axios";
import { useContext, useState } from "react";
import { useParams } from "react-router-dom";
import useSWR from "swr";
import { GlobalContext } from "../../../App";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { ActorResp, getActor } from "../../../service/actor";

export const useFetchActor = (actorId: string | null) => {
  return useSWR(
    actorId ? ["useActorDetail", actorId] : null,
    async ([_, actorId]) => {
      try {
        const actor_resp = await getActor(actorId);
        const data: ActorResp = actor_resp?.data;
        const { data: rspData } = data;

        if (rspData.detail) {
          return rspData.detail;
        }
      } catch (e) {
        // The API returns 404 for an unknown actor ID, which axios rejects on.
        if (axios.isAxiosError(e) && e.response?.status === 404) {
          return undefined;
        }
        throw e;
      }
    },
  );
};

export const useActorDetail = () => {
  const params = useParams() as { actorId: string };
  const [msg, setMsg] = useState("Loading the actor infos...");
  const { namespaceMap } = useContext(GlobalContext);

  const { data: actorDetail, isLoading } = useSWR(
    ["useActorDetail", params.actorId],
    async ([_, actorId]) => {
      try {
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
      } catch (e) {
        // The API returns 404 for an unknown actor ID, which axios rejects on.
        if (axios.isAxiosError(e) && e.response?.status === 404) {
          setMsg("Actor Query Error Please Check Actor Id");
          return undefined;
        }
        throw e;
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  return {
    params,
    actorDetail,
    msg,
    isLoading,
    namespaceMap,
  };
};
