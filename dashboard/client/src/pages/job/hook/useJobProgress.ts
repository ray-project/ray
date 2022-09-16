import { useCallback, useEffect, useRef, useState } from "react";
import { getJobProgress } from "../../../service/job";
import { TaskProgress } from "../../../type/job";

export const useJobProgress = () => {
  const [progress, setProgress] = useState<TaskProgress>();
  const [msg, setMsg] = useState("Loading progress...");
  const [isRefreshing, setRefresh] = useState(true);
  const refreshRef = useRef(isRefreshing);
  const tot = useRef<NodeJS.Timeout>();
  const onSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRefresh(event.target.checked);
  };
  refreshRef.current = isRefreshing;
  const getProgress = useCallback(async () => {
    if (!refreshRef.current) {
      return;
    }
    const rsp = await getJobProgress();

    if (rsp) {
      setProgress(rsp.data.data.detail);
      setMsg("Fetched jobs");
    }

    tot.current = setTimeout(getProgress, 4000);
  }, []);

  useEffect(() => {
    getProgress();
    return () => {
      if (tot.current) {
        clearTimeout(tot.current);
      }
    };
  }, [getProgress]);
  return {
    progress,
    msg,
    isRefreshing,
    onSwitchChange,
  };
};
