import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getClusterStatusMap } from "../../../service/node";

export const useClusterStatusMap = () => {
  const { data: clusterStatusMap } = useSWR(
    "useClusterStatusMap",
    async () => {
      try {
        const rsp = await getClusterStatusMap();
        return rsp.data.data;
      } catch (e) {
        console.error(
          "Cluster Error. Couldn't get the logical resource data from the dashboard server.",
        );
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  return {
    clusterStatusMap,
  };
};
