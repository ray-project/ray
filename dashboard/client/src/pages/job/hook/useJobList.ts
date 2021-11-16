import { useCallback, useEffect, useRef, useState } from "react";
import { getJobList } from "../../../service/job";
import { Job } from "../../../type/job";

export const useJobList = () => {
  const [jobList, setList] = useState<Job[]>([]);
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const [msg, setMsg] = useState("Loading the job list...");
  const [isRefreshing, setRefresh] = useState(true);
  const [filter, setFilter] = useState<
    {
      key: "jobId" | "name" | "language" | "state" | "namespaceId";
      val: string;
    }[]
  >([]);
  const refreshRef = useRef(isRefreshing);
  const tot = useRef<NodeJS.Timeout>();
  const changeFilter = (
    key: "jobId" | "name" | "language" | "state" | "namespaceId",
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
  refreshRef.current = isRefreshing;
  const getJob = useCallback(async () => {
    if (!refreshRef.current) {
      return;
    }
    const rsp = await getJobList();

    if (rsp?.data?.data?.summary) {
      setList(rsp.data.data.summary.sort((a, b) => b.timestamp - a.timestamp));
      setMsg(rsp.data.msg || "");
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
      filter.every((f) => node[f.key] && node[f.key].includes(f.val)),
    ),
    msg,
    isRefreshing,
    onSwitchChange,
    changeFilter,
    page,
    originalJobs: jobList,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
  };
};
