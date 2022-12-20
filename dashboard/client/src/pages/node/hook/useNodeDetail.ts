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

  const { data: nodeDetail } = useSWR(
    ["useNodeDetail", params.id],
    async (_, nodeId) => {
      const { data } = await getNodeDetail(nodeId);
      const { data: rspData, msg, result } = data;

      if (msg) {
        setMsg(msg);
      }

      if (result === false) {
        setMsg("Node Query Error Please Check Node Name");
        setRefresh(false);
      }

      if (rspData?.detail) {
        return rspData.detail;
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
    isRefreshing,
    onRefreshChange,
    raylet,
    handleChange,
    namespaceMap,
  };
};
