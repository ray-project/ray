import { useCallback, useContext, useEffect, useRef, useState } from "react";
import { GlobalContext } from "../../../App";
import { getJobList } from "../../../service/job";
import { UnifiedJob } from "../../../type/job";

export const useJobList = () => {
  const [jobList, setList] = useState<UnifiedJob[]>([]);
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const [msg, setMsg] = useState("Loading the job list...");
  const [isRefreshing, setRefresh] = useState(true);
  const { ipLogMap } = useContext(GlobalContext);
  const [filter, setFilter] = useState<
    {
      key: "job_id" | "status";
      val: string;
    }[]
  >([]);
  const refreshRef = useRef(isRefreshing);
  const tot = useRef<NodeJS.Timeout>();
  const changeFilter = (key: "job_id" | "status", val: string) => {
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
  refreshRef.current = isRefreshing;
  const getJob = useCallback(async () => {
    if (!refreshRef.current) {
      return;
    }
    const rsp = await getJobList();

    if (rsp) {
      setList(
        rsp.data.sort((a, b) => (b.start_time ?? 0) - (a.start_time ?? 0)),
      );
      setMsg("Fetched jobs");
    }

    tot.current = setTimeout(getJob, 4000);
  }, []);

  useEffect(() => {
    getJob();
    return () => {
      if (tot.current) {
        clearTimeout(tot.current);
      }
    };
  }, [getJob]);
  return {
    jobList: jobList.filter((node) =>
      filter.every((f) => node[f.key] && (node[f.key] ?? "").includes(f.val)),
    ),
    msg,
    isRefreshing,
    onSwitchChange,
    changeFilter,
    page,
    originalJobs: jobList,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
    ipLogMap,
  };
};
