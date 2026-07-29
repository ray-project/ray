import _ from "lodash";
import { useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { sliceToPage } from "../../../common/util";
import {
  getStateApiJobProgressByLineage,
  getStateApiJobProgressByTaskName,
} from "../../../service/job";
import {
  JobProgressGroup,
  NestedJobProgress,
  StateApiJobProgressByTaskName,
  StateApiNestedJobProgress,
  TaskProgress,
} from "../../../type/job";
import { TypeTaskStatus } from "../../../type/task";

export enum TaskStatus {
  PENDING_ARGS_AVAIL = "PENDING_ARGS_AVAIL",
  PENDING_NODE_ASSIGNMENT = "PENDING_NODE_ASSIGNMENT",
  SUBMITTED_TO_WORKER = "SUBMITTED_TO_WORKER",
  RUNNING = "RUNNING",
  FINISHED = "FINISHED",
  FAILED = "FAILED",
  UNKNOWN = "UNKNOWN",
}

const TASK_STATE_NAME_TO_PROGRESS_KEY: Record<TypeTaskStatus, TaskStatus> = {
  [TypeTaskStatus.PENDING_ARGS_AVAIL]: TaskStatus.PENDING_ARGS_AVAIL,
  [TypeTaskStatus.PENDING_NODE_ASSIGNMENT]: TaskStatus.PENDING_NODE_ASSIGNMENT,
  [TypeTaskStatus.PENDING_OBJ_STORE_MEM_AVAIL]:
    TaskStatus.PENDING_NODE_ASSIGNMENT,
  [TypeTaskStatus.PENDING_ARGS_FETCH]: TaskStatus.PENDING_NODE_ASSIGNMENT,
  [TypeTaskStatus.SUBMITTED_TO_WORKER]: TaskStatus.SUBMITTED_TO_WORKER,
  [TypeTaskStatus.PENDING_ACTOR_TASK_ARGS_FETCH]:
    TaskStatus.SUBMITTED_TO_WORKER,
  [TypeTaskStatus.PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY]:
    TaskStatus.SUBMITTED_TO_WORKER,
  [TypeTaskStatus.RUNNING]: TaskStatus.RUNNING,
  [TypeTaskStatus.RUNNING_IN_RAY_GET]: TaskStatus.RUNNING,
  [TypeTaskStatus.RUNNING_IN_RAY_WAIT]: TaskStatus.RUNNING,
  [TypeTaskStatus.FINISHED]: TaskStatus.FINISHED,
  [TypeTaskStatus.FAILED]: TaskStatus.FAILED,
  [TypeTaskStatus.NIL]: TaskStatus.UNKNOWN,
};

export const TaskStatusToTaskProgressMapping: Record<
  TaskStatus,
  keyof TaskProgress
> = {
  [TaskStatus.PENDING_ARGS_AVAIL]: "numPendingArgsAvail",
  [TaskStatus.PENDING_NODE_ASSIGNMENT]: "numPendingNodeAssignment",
  [TaskStatus.SUBMITTED_TO_WORKER]: "numSubmittedToWorker",
  [TaskStatus.RUNNING]: "numRunning",
  [TaskStatus.FINISHED]: "numFinished",
  [TaskStatus.FAILED]: "numFailed",
  [TaskStatus.UNKNOWN]: "numUnknown",
};

const useFetchStateApiProgressByTaskName = (
  jobId: string | undefined,
  isRefreshing: boolean,
  setMsg: (msg: string) => void,
  setError: (error: boolean) => void,
  setRefresh: (refresh: boolean) => void,
  disableRefresh: boolean,
  setLatestFetchTimestamp?: (time: number) => void,
) => {
  return useSWR(
    jobId ? ["useJobProgressByTaskName", jobId] : null,
    async ([_, jobId]) => {
      const rsp = await getStateApiJobProgressByTaskName(jobId);
      setMsg(rsp.data.msg);

      if (rsp.data.result) {
        setLatestFetchTimestamp?.(new Date().getTime());
        const summary = formatSummaryToTaskProgress(
          rsp.data.data.result.result,
        );
        return { summary, totalTasks: rsp.data.data.result.num_filtered };
      } else {
        setError(true);
        setRefresh(false);
      }
    },
    {
      refreshInterval:
        isRefreshing && !disableRefresh ? API_REFRESH_INTERVAL_MS : 0,
      revalidateOnFocus: false,
    },
  );
};

/**
 * Hook for fetching a job's task progress.
 * Refetches every 4 seconds unless refresh switch is toggled off.
 *
 * If jobId is undefined, we will not fetch the job progress.
 * @param jobId The id of the job whose task progress to fetch or undefined
 *              to fetch all progress for all jobs
 */
export const useJobProgress = (
  jobId: string | undefined,
  disableRefresh = false,
) => {
  const [msg, setMsg] = useState("Loading progress...");
  const [error, setError] = useState(false);
  const [isRefreshing, setRefresh] = useState(true);
  const [latestFetchTimestamp, setLatestFetchTimestamp] = useState(0);
  const { data, isLoading } = useFetchStateApiProgressByTaskName(
    jobId,
    isRefreshing,
    setMsg,
    setError,
    setRefresh,
    disableRefresh,
    setLatestFetchTimestamp,
  );

  const summed = (data?.summary ?? []).reduce((acc, task) => {
    Object.entries(task.progress).forEach(([k, count]) => {
      const key = k as keyof TaskProgress;
      acc[key] = (acc[key] ?? 0) + count;
    });
    return acc;
  }, {} as TaskProgress);

  const driverExists = !jobId ? false : true;
  return {
    progress: summed,
    totalTasks: data?.totalTasks,
    isLoading,
    msg,
    error,
    driverExists,
    latestFetchTimestamp,
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

  const { data, isLoading } = useFetchStateApiProgressByTaskName(
    jobId,
    isRefreshing,
    setMsg,
    setError,
    setRefresh,
    false,
  );

  const formattedTasks = (data?.summary ?? []).map((task) => {
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
  const paginatedTasks = sliceToPage(sortedTasks, page).items;

  return {
    progress: paginatedTasks,
    page: { pageNo: page, pageSize: 10 },
    total: formattedTasks.length,
    totalTasks: data?.totalTasks,
    isLoading,
    setPage,
    msg,
    error,
    onSwitchChange,
  };
};

const formatStateCountsToProgress = (stateCounts: {
  [stateName: string]: number;
}) => {
  const formattedProgress: TaskProgress = {};
  Object.entries(stateCounts).forEach(([state, count]) => {
    const taskStatus: TaskStatus =
      TASK_STATE_NAME_TO_PROGRESS_KEY[state as TypeTaskStatus];

    const key: keyof TaskProgress =
      TaskStatusToTaskProgressMapping[taskStatus] ?? "numUnknown";

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

const formatToJobProgressGroup = (
  nestedJobProgress: NestedJobProgress,
  showFinishedTasks = true,
): JobProgressGroup | undefined => {
  const formattedProgress = formatStateCountsToProgress(
    nestedJobProgress.state_counts,
  );

  const total = Object.values(formattedProgress).reduce(
    (acc, count) => acc + count,
    0,
  );
  if (
    !showFinishedTasks &&
    total - (formattedProgress.numFinished ?? 0) === 0
  ) {
    return undefined;
  }

  return {
    name: nestedJobProgress.name,
    key: nestedJobProgress.key,
    progress: formattedProgress,
    children: nestedJobProgress.children
      .map((child) => formatToJobProgressGroup(child, showFinishedTasks))
      .filter((child): child is JobProgressGroup => child !== undefined),
    type: nestedJobProgress.type,
    link: nestedJobProgress.link,
  };
};

export const formatNestedJobProgressToJobProgressGroup = (
  summary: StateApiNestedJobProgress,
  showFinishedTasks = true,
) => {
  const tasks = summary.node_id_to_summary.cluster.summary;
  const progressGroups = tasks
    .map((task) => formatToJobProgressGroup(task, showFinishedTasks))
    .filter((group): group is JobProgressGroup => group !== undefined);

  const total = tasks.reduce<TaskProgress>((acc, group) => {
    const formattedProgress = formatStateCountsToProgress(group.state_counts);
    Object.entries(formattedProgress).forEach(([key, count]) => {
      const progressKey = key as keyof TaskProgress;
      acc[progressKey] = (acc[progressKey] ?? 0) + count;
    });
    return acc;
  }, {});

  return { progressGroups, total };
};

/**
 * Hook for fetching a job's task progress grouped by lineage. This is
 * used for the Advanced progress bar.
 * Refetches every 4 seconds.
 *
 * @param jobId The id of the job whose task progress to fetch or undefined
 *              to fetch all progress for all jobs
 *              If null, we will avoid fetching.
 */
export const useJobProgressByLineage = (
  jobId: string | undefined,
  disableRefresh = false,
  showFinishedTasks = true,
) => {
  const [msg, setMsg] = useState("Loading progress...");
  const [error, setError] = useState(false);
  const [isRefreshing, setRefresh] = useState(true);
  const [latestFetchTimestamp, setLatestFetchTimestamp] = useState(0);

  const { data, isLoading } = useSWR(
    jobId ? ["useJobProgressByLineageAndName", jobId, showFinishedTasks] : null,
    async ([_, jobId, showFinishedTasks]) => {
      const rsp = await getStateApiJobProgressByLineage(jobId);
      setMsg(rsp.data.msg);

      if (rsp.data.result) {
        setLatestFetchTimestamp(new Date().getTime());
        const summary = formatNestedJobProgressToJobProgressGroup(
          rsp.data.data.result.result,
          showFinishedTasks,
        );
        return { summary, totalTasks: rsp.data.data.result.num_filtered };
      } else {
        setError(true);
        setRefresh(false);
      }
    },
    {
      refreshInterval:
        isRefreshing && !disableRefresh ? API_REFRESH_INTERVAL_MS : 0,
      revalidateOnFocus: false,
    },
  );

  return {
    progressGroups: data?.summary?.progressGroups,
    total: data?.summary?.total,
    totalTasks: data?.totalTasks,
    isLoading,
    msg,
    error,
    latestFetchTimestamp,
  };
};
