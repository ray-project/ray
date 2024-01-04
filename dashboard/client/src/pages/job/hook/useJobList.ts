import { useRef, useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getJobList } from "../../../service/job";

export const useJobList = () => {
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const [msg, setMsg] = useState("Loading the job list...");
  const [isRefreshing, setRefresh] = useState(true);
  const [filter, setFilter] = useState<
    {
      key: "job_id" | "status";
      val: string;
    }[]
  >([]);
  const refreshRef = useRef(isRefreshing);
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

  const { data, isLoading } = useSWR(
    "useJobList",
    async () => {
      const rsp = await getJobList();

      if (rsp) {
        setMsg("Fetched jobs");
        return rsp.data.sort(
          (a, b) => (b.start_time ?? 0) - (a.start_time ?? 0),
        );
      }
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  const jobList = data ?? [];

  return {
    jobList: jobList.filter((node) =>
      filter.every((f) => node[f.key] && (node[f.key] ?? "").includes(f.val)),
    ),
    msg,
    isLoading,
    isRefreshing,
    onSwitchChange,
    changeFilter,
    page,
    originalJobs: jobList,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
  };
};
