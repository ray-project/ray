import { useCallback, useEffect, useRef, useState } from "react";
import { getNodeList } from "../../../service/node";
import { NodeDetail } from "../../../type/node";
import { useSorter } from "../../../util/hook";

export const useNodeList = () => {
  const [nodeList, setList] = useState<NodeDetail[]>([]);
  const [msg, setMsg] = useState("Loading the nodes infos...");
  const [isRefreshing, setRefresh] = useState(true);
  const [mode, setMode] = useState("table");
  const [filter, setFilter] = useState<
    { key: "hostname" | "ip" | "state"; val: string }[]
  >([]);
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const { sorterFunc, setOrderDesc, setSortKey, sorterKey } = useSorter("cpu");
  const tot = useRef<NodeJS.Timeout>();
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
  const getList = useCallback(async () => {
    if (!isRefreshing) {
      return;
    }
    const { data } = await getNodeList();
    const { data: rspData, msg } = data;
    setList(rspData.summary || []);
    if (msg) {
      setMsg(msg);
    } else {
      setMsg("");
    }
    tot.current = setTimeout(getList, 4000);
  }, [isRefreshing]);

  useEffect(() => {
    getList();
    return () => {
      if (tot.current) {
        clearTimeout(tot.current);
      }
    };
  }, [getList]);

  return {
    nodeList: nodeList
      .map((e) => ({ ...e, state: e.raylet.state }))
      .sort((a, b) => (a.raylet.nodeId > b.raylet.nodeId ? 1 : -1))
      .sort(sorterFunc)
      .filter((node) =>
        filter.every((f) => node[f.key] && node[f.key].includes(f.val)),
      ),
    msg,
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
