import { useRef, useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getSubmitList } from "../../../service/submit";

export const useSubmitList = () => {
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const [msg, setMsg] = useState("Loading the submit list...");
  const [isRefreshing, setRefresh] = useState(true);
  const [filter, setFilter] = useState<
    {
      key: "submission_id" | "status";
      val: string;
    }[]
  >([]);
  const refreshRef = useRef(isRefreshing);
  const changeFilter = (key: "submission_id" | "status", val: string) => {
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
    "useSubmissionList",
    async () => {
      const rsp = await getSubmitList();

      if (rsp) {
        setMsg("Fetched submissions");
        return rsp.data.sort(
          (a, b) => (b.start_time ?? 0) - (a.start_time ?? 0),
        );
      }
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  const submitList = data ?? [];

  return {
    submitList: submitList.filter((node) =>
      filter.every((f) => node[f.key] && (node[f.key] ?? "").includes(f.val)),
    ),
    msg,
    isLoading,
    isRefreshing,
    onSwitchChange,
    changeFilter,
    page,
    originalJobs: submitList,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
  };
};
