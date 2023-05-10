import { Typography } from "@material-ui/core";
import React from "react";
import useSWR from "swr";
import { getStateApiDownloadLogUrl, getStateApiLog } from "../../service/log";
import { UnifiedJob } from "../../type/job";
import { LogViewer } from "../log/LogViewer";

const useDriverLogs = (
  job: Pick<UnifiedJob, "driver_node_id" | "submission_id">,
) => {
  const { driver_node_id, submission_id } = job;

  const filename = submission_id
    ? `job-driver-${submission_id}.log`
    : undefined;

  const downloadUrl =
    driver_node_id && filename
      ? getStateApiDownloadLogUrl(driver_node_id, filename)
      : undefined;

  const {
    data: log,
    isLoading,
    mutate,
  } = useSWR(
    driver_node_id && filename
      ? ["useDriverLogs", driver_node_id, filename]
      : null,
    async ([_, node_id, filename]) => {
      return getStateApiLog(node_id, filename);
    },
  );

  return {
    log: isLoading ? "Loading..." : log,
    downloadUrl,
    refresh: mutate,
    path: filename,
  };
};

type JobDriverLogsProps = {
  job: Pick<UnifiedJob, "driver_node_id" | "submission_id">;
};

export const JobDriverLogs = ({ job }: JobDriverLogsProps) => {
  const { downloadUrl, log, path, refresh } = useDriverLogs(job);
  return typeof log === "string" ? (
    <LogViewer
      log={log}
      path={path}
      downloadUrl={downloadUrl}
      height={300}
      onRefreshClick={() => {
        refresh();
      }}
    />
  ) : (
    <Typography color="error">Failed to load</Typography>
  );
};
