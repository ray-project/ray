import { Typography } from "@material-ui/core";
import React, { useContext } from "react";
import useSWR from "swr";
import { GlobalContext } from "../../App";
import { getLogDetail, getLogDownloadUrl } from "../../service/log";
import { UnifiedJob } from "../../type/job";
import { LogViewer } from "../log/LogViewer";

const useDriverLogs = (
  job: Pick<
    UnifiedJob,
    "driver_agent_http_address" | "driver_info" | "submission_id"
  >,
) => {
  const { ipLogMap } = useContext(GlobalContext);
  const { driver_agent_http_address, driver_info, submission_id } = job;
  const host = (() => {
    if (driver_agent_http_address) {
      return `${driver_agent_http_address}/logs/`;
    } else if (driver_info && ipLogMap[driver_info.node_ip_address]) {
      return `${ipLogMap[driver_info.node_ip_address]}/`;
    }
  })();
  const path = `job-driver-${submission_id}.log`;

  const url = host ? `${host}${path}` : undefined;
  const downloadUrl = url ? getLogDownloadUrl(url) : undefined;

  const {
    data: log,
    isLoading,
    mutate,
  } = useSWR(url ? ["useDriverLogs", url] : null, async ([_, url]) =>
    getLogDetail(url)
      .then((res) => {
        if (res) {
          return res;
        } else {
          return "(This file is empty.)";
        }
      })
      .catch(() => {
        return "(Failed to load)";
      }),
  );

  return {
    log: isLoading ? "Loading..." : log,
    downloadUrl,
    refresh: mutate,
    host,
    path,
  };
};

type JobDriverLogsProps = {
  job: Pick<
    UnifiedJob,
    "driver_agent_http_address" | "driver_info" | "submission_id"
  >;
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
