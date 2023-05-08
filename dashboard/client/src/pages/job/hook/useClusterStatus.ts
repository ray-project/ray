import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getRayStatus } from "../../../service/status";

export const useRayStatus = () => {
  const { data: cluster_status } = useSWR(
    "useClusterStatus",
    async () => {
      try {
        const rsp = await getRayStatus();
        return rsp.data;
      } catch (e) {
        console.error(
          "Cluster Status Error. Couldn't get the cluster status data from the dashboard server.",
        );
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  return {
    cluster_status,
  };
};
