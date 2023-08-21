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
    { key: "hostname" | "ip" | "state"; val: string }[]
  >([]);
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const { sorterFunc, setOrderDesc, setSortKey, sorterKey } = useSorter("");
  const changeFilter = (key: "hostname" | "ip" | "state", val: string) => {
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

  const nodeListWithAdditionalInfo = nodeList
    .map((e) => ({
      ...e,
      state: e.raylet.state,
      logicalResources: nodeLogicalResources[e.raylet.nodeId],
    }))
    .sort(sorterFunc);

  const sortedList = _.sortBy(nodeListWithAdditionalInfo, [
    (obj) => !obj.raylet.isHeadNode,
    // sort by alive first, then alphabetically for other states
    (obj) => (obj.raylet.state === "ALIVE" ? "0" : obj.raylet.state),
    (obj) => obj.raylet.nodeId,
  ]);

  return {
    nodeList: sortedList.filter((node) =>
      filter.every((f) => node[f.key] && node[f.key].includes(f.val)),
    ),
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
