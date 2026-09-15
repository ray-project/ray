import axios from "axios";
import { useContext, useState } from "react";
import { useParams } from "react-router-dom";
import useSWR from "swr";
import { GlobalContext } from "../../../App";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getNodeDetail } from "../../../service/node";

export const useNodeDetail = () => {
  const params = useParams() as { id: string };
  const [selectedTab, setTab] = useState("info");
  const [msg, setMsg] = useState("Loading the node infos...");
  const { namespaceMap } = useContext(GlobalContext);
  const [isRefreshing, setRefresh] = useState(true);
  const onRefreshChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRefresh(event.target.checked);
  };

  const { data: nodeDetail, isLoading } = useSWR(
    ["useNodeDetail", params.id],
    async ([_, nodeId]) => {
      try {
        const { data } = await getNodeDetail(nodeId);
        const { data: rspData, msg } = data;

        if (msg) {
          setMsg(msg);
        }

        if (rspData?.detail) {
          return rspData.detail;
        }
      } catch (e) {
        // The API returns 404 for an unknown node ID, which axios rejects on.
        // Keep auto-refresh running: a 404 can be transient (e.g. the node is
        // not in the dashboard's node table yet), and stopping the refresh
        // would leave the page stuck on the error until a manual reload.
        if (axios.isAxiosError(e) && e.response?.status === 404) {
          setMsg("Node Query Error Please Check Node Name");
          return undefined;
        }
        throw e;
      }
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  const raylet = nodeDetail?.raylet;
  const handleChange = (event: React.ChangeEvent<{}>, newValue: string) => {
    setTab(newValue);
  };

  return {
    params,
    selectedTab,
    nodeDetail,
    msg,
    isLoading,
    isRefreshing,
    onRefreshChange,
    raylet,
    handleChange,
    namespaceMap,
  };
};
