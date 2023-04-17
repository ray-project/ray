import { useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getClusterMetadata } from "../../../service/global";

export const useClusterDetail = () => {
  const [msg, setMsg] = useState("Loading the job detail");
  const [refreshing, setRefresh] = useState(true);
  const { data: clusterDetail } = useSWR(
    "useClusterDetail",
    async () => {
      try {
        const rsp = await getClusterMetadata();
        return rsp.data;
      } catch (e) {
        setMsg("Cluster Detail Query Error.");
        setRefresh(false);
      }
    },
    { refreshInterval: refreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  return {
    clusterDetail,
    msg,
  };
};
