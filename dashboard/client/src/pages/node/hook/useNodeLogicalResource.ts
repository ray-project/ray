import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getNodeLogicalResourceMap } from "../../../service/node";

export const useNodeLogicalResourceMap = () => {
  const { data: nodeLogicalResourceMap } = useSWR(
    "useNodeLogicalResourceMap",
    async () => {
      try {
        const rsp = await getNodeLogicalResourceMap();
        console.info("rsp: ", rsp);
        return rsp.data;
      } catch (e) {
        console.error(
          "Cluster Error. Couldn't get the logical resource data from the dashboard server.",
        );
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  return {
    nodeLogicalResourceMap,
  };
};
