import { Typography } from "@material-ui/core";
import React, { useContext, useEffect, useState } from "react";
import { GlobalContext } from "../../App";
import { getLogDetail, getLogDownloadUrl } from "../../service/log";
import { UnifiedJob } from "../../type/job";
import { LogViewer } from "../log/LogViewer";

// https://session-ea89fl9yr56lxmwrtwywn44yv7.i.anyscaleuserdata-staging.com/log_proxy?url=http%3A%2F%2F10.0.59.114%3A52365%2Flogs%2Fdashboard_agent.log

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

  const [log, setLogs] =
    useState<undefined | string | { [key: string]: string }[]>();
  const [downloadUrl, setDownloadUrl] = useState<string>();

  useEffect(() => {
    setLogs("Loading...");
    if (host) {
      let url = host;
      if (path) {
        url += path;
      }
      setDownloadUrl(getLogDownloadUrl(url));
      getLogDetail(url)
        .then((res) => {
          if (res) {
            setLogs(res);
          } else {
            setLogs("(null)");
          }
        })
        .catch(() => {
          setLogs("Failed to load");
        });
    } else {
      setLogs("Failed to load");
    }
  }, [host, path]);

  return {
    log,
    downloadUrl,
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
  const { downloadUrl, log, path } = useDriverLogs(job);
  return typeof log === "string" ? (
    <LogViewer log={log} path={path} downloadUrl={downloadUrl} />
  ) : (
    <Typography color="error">Failed to load</Typography>
  );
};
