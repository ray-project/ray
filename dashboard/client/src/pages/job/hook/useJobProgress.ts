import _ from "lodash";
import { useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import {
  getStateApiJobProgressByLineage,
  getStateApiJobProgressByTaskName,
} from "../../../service/job";
import {
  JobProgressByLineage,
  LineageSummary,
  StateApiJobProgressByLineage,
  StateApiJobProgressByTaskName,
  TaskProgress,
} from "../../../type/job";
import { TypeTaskStatus } from "../../../type/task";

const TASK_STATE_NAME_TO_PROGRESS_KEY: Record<
  TypeTaskStatus,
  keyof TaskProgress
> = {
  [TypeTaskStatus.PENDING_ARGS_AVAIL]: "numPendingArgsAvail",
  [TypeTaskStatus.PENDING_NODE_ASSIGNMENT]: "numPendingNodeAssignment",
  [TypeTaskStatus.PENDING_OBJ_STORE_MEM_AVAIL]: "numPendingNodeAssignment",
  [TypeTaskStatus.PENDING_ARGS_FETCH]: "numPendingNodeAssignment",
  [TypeTaskStatus.SUBMITTED_TO_WORKER]: "numSubmittedToWorker",
  [TypeTaskStatus.RUNNING]: "numRunning",
  [TypeTaskStatus.RUNNING_IN_RAY_GET]: "numRunning",
  [TypeTaskStatus.RUNNING_IN_RAY_WAIT]: "numRunning",
  [TypeTaskStatus.FINISHED]: "numFinished",
  [TypeTaskStatus.FAILED]: "numFailed",
  [TypeTaskStatus.NIL]: "numUnknown",
};

const useFetchStateApiProgressByTaskName = (
  jobId: string | undefined,
  isRefreshing: boolean,
  setMsg: (msg: string) => void,
  setError: (error: boolean) => void,
  setRefresh: (refresh: boolean) => void,
) => {
  return useSWR(
    jobId ? ["useJobProgressByTaskName", jobId] : null,
    async (_, jobId) => {
      const rsp = await getStateApiJobProgressByTaskName(jobId);
      setMsg(rsp.data.msg);

      if (rsp.data.result) {
        return formatSummaryToTaskProgress(rsp.data.data.result.result);
      } else {
        setError(true);
        setRefresh(false);
      }
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );
};

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
  const onSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRefresh(event.target.checked);
  };
  const { data: tasks } = useFetchStateApiProgressByTaskName(
    jobId,
    isRefreshing,
    setMsg,
    setError,
    setRefresh,
  );

  const summed = (tasks ?? []).reduce((acc, task) => {
    Object.entries(task.progress).forEach(([k, count]) => {
      const key = k as keyof TaskProgress;
      acc[key] = (acc[key] ?? 0) + count;
    });
    return acc;
  }, {} as TaskProgress);

  const driverExists = !jobId ? false : true;
  return {
    progress: summed,
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
  const onSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRefresh(event.target.checked);
  };

  const { data: tasks } = useFetchStateApiProgressByTaskName(
    jobId,
    isRefreshing,
    setMsg,
    setError,
    setRefresh,
  );

  const formattedTasks = (tasks ?? []).map((task) => {
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
    formattedTasks,
    ["numFailed", "numActive", "numFinished"],
    ["desc", "desc", "desc"],
  );
  const paginatedTasks = sortedTasks.slice((page - 1) * 10, page * 10);

  return {
    progress: paginatedTasks,
    page: { pageNo: page, pageSize: 10 },
    total: formattedTasks.length,
    setPage,
    msg,
    error,
    isRefreshing,
    onSwitchChange,
  };
};

const formatStateCountsToProgress = (stateCounts: {
  [stateName: string]: number;
}) => {
  const formattedProgress: TaskProgress = {};
  Object.entries(stateCounts).forEach(([state, count]) => {
    const key: keyof TaskProgress =
      TASK_STATE_NAME_TO_PROGRESS_KEY[state as TypeTaskStatus] ?? "numUnknown";

    formattedProgress[key] = (formattedProgress[key] ?? 0) + count;
  });

  return formattedProgress;
};

export const formatSummaryToTaskProgress = (
  summary: StateApiJobProgressByTaskName,
) => {
  const tasks = summary.node_id_to_summary.cluster.summary;
  const formattedTasks = Object.entries(tasks).map(([name, task]) => {
    const formattedProgress = formatStateCountsToProgress(task.state_counts);
    return { name, progress: formattedProgress };
  });

  return formattedTasks;
};

const formatLineageSummaryToTaskProgress = (
  lineageSummary: LineageSummary,
): JobProgressByLineage => {
  const formattedProgress = formatStateCountsToProgress(
    lineageSummary.state_counts,
  );
  const name = lineageSummary.lineage.at(-1);
  if (name === undefined) {
    throw new Error("Unexpected empty lineage from API");
  }
  return {
    name,
    key: lineageSummary.lineage.join(","),
    lineage: lineageSummary.lineage,
    progress: formattedProgress,
    children: lineageSummary.children.map(formatLineageSummaryToTaskProgress),
  };
};

export const formatLineageSummariesToTaskProgress = (
  summary: StateApiJobProgressByLineage,
) => {
  const tasks = summary.node_id_to_summary.cluster.summary;
  const formattedTasks = Object.values(tasks).map(
    formatLineageSummaryToTaskProgress,
  );

  return formattedTasks;
};

/**
 * Hook for fetching a job's task progress grouped by lineage. This is
 * used for the Advanced progress bar.
 * Refetches every 4 seconds.
 *
 * @param jobId The id of the job whose task progress to fetch or undefined
 *              to fetch all progress for all jobs
 */
export const useJobProgressByLineage = (jobId: string) => {
  const [msg, setMsg] = useState("Loading progress...");
  const [error, setError] = useState(false);
  const [isRefreshing, setRefresh] = useState(true);
  const onSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRefresh(event.target.checked);
  };

  const { data: tasks } = useSWR(
    jobId ? ["useJobProgressByLineage", jobId] : null,
    async (_, jobId) => {
      const rsp = await getStateApiJobProgressByLineage(jobId);
      setMsg(rsp.data.msg);

      if (rsp.data.result) {
        return formatLineageSummariesToTaskProgress(
          rsp.data.data.result.result,
        );
      } else {
        setError(true);
        setRefresh(false);
      }
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  return {
    progress: tasks,
    msg,
    error,
    isRefreshing,
    onSwitchChange,
  };
};
