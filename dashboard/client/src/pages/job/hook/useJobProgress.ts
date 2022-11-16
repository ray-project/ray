import _ from "lodash";
import { useCallback, useEffect, useRef, useState } from "react";
import { getJobProgress, getJobProgressByTaskName } from "../../../service/job";
import { JobProgressByTaskName, TaskProgress } from "../../../type/job";

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

/**
 * Hook for fetching a job's task progress grouped by task name.
 * Refetches every 4 seconds unless refresh switch is toggled off.
 *
 * If jobId is not provided, will fetch the task progress across all jobs.
 * @param jobId The id of the job whose task progress to fetch or undefined
 *              to fetch all progress for all jobs
 */
export const useJobProgressByTaskName = (jobId: string) => {
  const [progress, setProgress] = useState<JobProgressByTaskName>();
  const [page, setPage] = useState(1);
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
    const rsp = await getJobProgressByTaskName(jobId);

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
