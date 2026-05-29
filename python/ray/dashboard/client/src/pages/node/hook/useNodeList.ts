import _ from "lodash";
import { useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getNodeList } from "../../../service/node";
import { useSorter } from "../../../util/hook";

export const useNodeList = () => {
  const [msg, setMsg] = useState("Loading the nodes infos...");
  const [isRefreshing, setRefresh] = useState(true);
  const [mode, setMode] = useState("table");
  const [filter, setFilter] = useState<
    { key: "hostname" | "ip" | "state" | "nodeId"; val: string }[]
  >([]);
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const { sorterFunc, setOrderDesc, setSortKey, sorterKey } = useSorter("");
  const changeFilter = (
    key: "hostname" | "ip" | "state" | "nodeId",
    val: string,
  ) => {
    const f = filter.find((e) => e.key === key);
    if (f) {
      f.val = val;
    } else {
      filter.push({ key, val });
    }
    setFilter([...filter]);
  };
  const onSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRefresh(event.target.checked);
  };
  const { data, isLoading } = useSWR(
    "useNodeList",
    async () => {
      const { data } = await getNodeList();
      const { data: rspData, msg } = data;
      if (msg) {
        setMsg(msg);
      } else {
        setMsg("");
      }
      return rspData;
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  const nodeList = data?.summary ?? [];
  const nodeLogicalResources = data?.nodeLogicalResources ?? {};

  const nodeListWithAdditionalInfo = nodeList.map((e) => ({
    ...e,
    state: e.raylet.state,
    logicalResources: nodeLogicalResources[e.raylet.nodeId],
  }));

  const sortedList = _.sortBy(nodeListWithAdditionalInfo, (node) => {
    // After sorting by user specified field, stable sort by
    // 1) Alive nodes first then alphabetically for other states
    // 2) Head node
    // 3) Alphabetical by node id
    const nodeStateOrder =
      node.raylet.state === "ALIVE" ? "0" : node.raylet.state;
    const isHeadNodeOrder = node.raylet.isHeadNode ? "0" : "1";
    const nodeIdOrder = node.raylet.nodeId;
    return [nodeStateOrder, isHeadNodeOrder, nodeIdOrder];
  }).sort(sorterFunc);

  const filteredList = sortedList.filter((node) => {
    const nodeId = node.raylet.nodeId;
    return filter.every((f) => {
      if (f.key === "nodeId") {
        return nodeId && nodeId.includes(f.val);
      } else {
        return node[f.key] && node[f.key].includes(f.val);
      }
    });
  });
  return {
    nodeList: filteredList,
    msg,
    isLoading,
    isRefreshing,
    onSwitchChange,
    changeFilter,
    page,
    originalNodes: nodeList,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
    sorterKey,
    setSortKey,
    setOrderDesc,
    mode,
    setMode,
  };
};
