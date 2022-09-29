import { useCallback, useEffect, useRef, useState } from "react";
import { getJobProgress } from "../../../service/job";
import { TaskProgress } from "../../../type/job";

/**
 * Hook for fetching a job's task progress.
 * Refetches every 4 seconds unless refresh switch is toggled off.
 *
 * If jobId is not provided, will fetch the task progress across all jobs.
 * @param jobId The id of the job whose task progress to fetch or undefined
 *              to fetch all progress for all jobs
 */
export const useJobProgress = (jobId?: string) => {
  const [progress, setProgress] = useState<TaskProgress>();
  const [msg, setMsg] = useState("Loading progress...");
  const [error, setError] = useState(false);
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
    const rsp = await getJobProgress(jobId);

    if (rsp) {
      if (rsp.data.result) {
        setProgress(rsp.data.data.detail);
      } else {
        setError(true);
      }
      setMsg(rsp.data.msg);
    }

    tot.current = setTimeout(getProgress, 4000);
  }, [jobId]);

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
    error,
    isRefreshing,
    onSwitchChange,
  };
};
