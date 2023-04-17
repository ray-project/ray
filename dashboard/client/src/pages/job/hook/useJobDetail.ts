import { useContext, useState } from "react";
import { useParams } from "react-router-dom";
import useSWR from "swr";
import { GlobalContext } from "../../../App";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getJobDetail } from "../../../service/job";

export const useJobDetail = () => {
  const params = useParams() as { id: string };
  const [msg, setMsg] = useState("Loading the job detail");
  const [refreshing, setRefresh] = useState(true);
  const { ipLogMap } = useContext(GlobalContext);
  const { data: job } = useSWR(
    "useJobDetail",
    async () => {
      try {
        const rsp = await getJobDetail(params.id);
        return rsp.data;
      } catch (e) {
        setMsg("Job Query Error Please Check JobId");
        setRefresh(false);
      }
    },
    { refreshInterval: refreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  return {
    job,
    msg,
    params,
    ipLogMap,
  };
};
