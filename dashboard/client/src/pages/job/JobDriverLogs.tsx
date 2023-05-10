import { Typography } from "@material-ui/core";
import React from "react";
import { UnifiedJob } from "../../type/job";
import { useStateApiLogs } from "../log/hooks";
import { LogViewer } from "../log/LogViewer";

type JobDriverLogsProps = {
  job: Pick<UnifiedJob, "driver_node_id" | "submission_id">;
};

export const JobDriverLogs = ({ job }: JobDriverLogsProps) => {
  const { driver_node_id, submission_id } = job;
  const filename = submission_id
    ? `job-driver-${submission_id}.log`
    : undefined;

  const { downloadUrl, log, path, refresh } = useStateApiLogs(
    driver_node_id,
    filename,
  );
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
