import _ from "lodash";
import { useRef, useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getJobProgress, getJobProgressByTaskName } from "../../../service/job";

/**
 * Hook for fetching a job's task progress.
 * Refetches every 4 seconds unless refresh switch is toggled off.
 *
 * If jobId is not provided, will fetch the task progress across all jobs.
 * @param jobId The id of the job whose task progress to fetch or undefined
 *              to fetch all progress for all jobs
 */
export const useJobProgress = (jobId?: string) => {
  const [msg, setMsg] = useState("Loading progress...");
  const [error, setError] = useState(false);
  const [isRefreshing, setRefresh] = useState(true);
  const refreshRef = useRef(isRefreshing);
  const onSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRefresh(event.target.checked);
  };
  refreshRef.current = isRefreshing;
  const { data: progress } = useSWR(
    jobId ? ["useJobProgress", jobId] : null,
    async (_, jobId) => {
      const rsp = await getJobProgress(jobId);

      setMsg(rsp.data.msg);
      if (rsp.data.result) {
        return rsp.data.data.detail;
      } else {
        setError(true);
        setRefresh(false);
      }
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  const driverExists = !jobId ? false : true;
  return {
    progress,
    msg,
    error,
    isRefreshing,
    onSwitchChange,
    driverExists,
  };
};

/**
 * Hook for fetching a job's task progress grouped by task name.
 * Refetches every 4 seconds unless refresh switch is toggled off.
 *
 * If jobId is not provided, will fetch the task progress across all jobs.
 * @param jobId The id of the job whose task progress to fetch or undefined
 *              to fetch all progress for all jobs
 */
export const useJobProgressByTaskName = (jobId: string) => {
  const [page, setPage] = useState(1);
  const [msg, setMsg] = useState("Loading progress...");
  const [error, setError] = useState(false);
  const [isRefreshing, setRefresh] = useState(true);
  const refreshRef = useRef(isRefreshing);
  const onSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRefresh(event.target.checked);
  };
  refreshRef.current = isRefreshing;

  const { data: progress } = useSWR(
    ["useJobProgressByTaskName", jobId],
    async (_, jobId) => {
      const rsp = await getJobProgressByTaskName(jobId);
      setMsg(rsp.data.msg);

      if (rsp.data.result) {
        return rsp.data.data.detail;
      } else {
        setError(true);
        setRefresh(false);
      }
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  const tasks = progress?.tasks ?? [];
  const tasksWithTotals = tasks.map((task) => {
    const {
      numFailed = 0,
      numPendingArgsAvail = 0,
      numPendingNodeAssignment = 0,
      numRunning = 0,
      numSubmittedToWorker = 0,
      numFinished = 0,
    } = task.progress;

    const numActive =
      numPendingArgsAvail +
      numPendingNodeAssignment +
      numRunning +
      numSubmittedToWorker;

    return { ...task, numFailed, numActive, numFinished };
  });
  const sortedTasks = _.orderBy(
    tasksWithTotals,
    ["numFailed", "numActive", "numFinished"],
    ["desc", "desc", "desc"],
  );
  const paginatedTasks = sortedTasks.slice((page - 1) * 10, page * 10);

  return {
    progress: paginatedTasks,
    page: { pageNo: page, pageSize: 10 },
    total: tasks.length,
    setPage,
    msg,
    error,
    isRefreshing,
    onSwitchChange,
  };
};
