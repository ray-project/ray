import { TableCell, TableRow, Tooltip } from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import dayjs from "dayjs";
import React, { useContext } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../../App";
import { DurationText } from "../../common/DurationText";
import { UnifiedJob } from "../../type/job";
import { useJobProgress } from "./hook/useJobProgress";
import { MiniTaskProgressBar } from "./TaskProgressBar";

const useStyles = makeStyles((theme) => ({
  overflowCell: {
    display: "block",
    width: "150px",
    textOverflow: "ellipsis",
    overflow: "hidden",
    whiteSpace: "nowrap",
  },
}));

type JobRowProps = {
  job: UnifiedJob;
  newIA?: boolean;
};

export const JobRow = ({
  job: {
    job_id,
    submission_id,
    driver_info,
    type,
    status,
    start_time,
    end_time,
    entrypoint,
    driver_agent_http_address,
  },
  newIA = false,
}: JobRowProps) => {
  const { ipLogMap } = useContext(GlobalContext);
  const { progress, error, driverExists } = useJobProgress(job_id ?? undefined);
  const classes = useStyles();

  const progressBar = (() => {
    if (!driverExists) {
      return <MiniTaskProgressBar />;
    }
    if (!progress || error) {
      return "unavailable";
    }
    if (status === "SUCCEEDED" || status === "FAILED") {
      // TODO(aguo): Show failed tasks in progress bar once supported.
      return <MiniTaskProgressBar {...progress} showAsComplete />;
    } else {
      return <MiniTaskProgressBar {...progress} />;
    }
  })();

  const logsLink = (() => {
    let link: string | undefined;
    if (driver_agent_http_address) {
      link = `/log/${encodeURIComponent(`${driver_agent_http_address}/logs`)}`;
    } else if (driver_info && ipLogMap[driver_info.node_ip_address]) {
      link = `/log/${encodeURIComponent(
        ipLogMap[driver_info.node_ip_address],
      )}`;
    }

    if (link) {
      link += `?fileName=${
        type === "DRIVER" ? job_id : `driver-${submission_id}`
      }`;
      return (
        <Link to={link} target="_blank">
          Log
        </Link>
      );
    }

    return "-";
  })();

  return (
    <TableRow>
      <TableCell align="center">
        {job_id ? (
          <Link to={newIA ? `${job_id}` : `/job/${job_id}`}>{job_id}</Link>
        ) : (
          "-"
        )}
      </TableCell>
      <TableCell align="center">{submission_id ?? "-"}</TableCell>
      <TableCell align="center">
        <Tooltip
          className={classes.overflowCell}
          title={entrypoint}
          arrow
          interactive
        >
          <div>{entrypoint}</div>
        </Tooltip>
      </TableCell>
      <TableCell align="center">{status}</TableCell>
      <TableCell align="center">
        {start_time && start_time > 0 ? (
          <DurationText startTime={start_time} endTime={end_time} />
        ) : (
          "-"
        )}
      </TableCell>
      <TableCell align="center">{progressBar}</TableCell>
      <TableCell align="center">
        {/* TODO(aguo): Also show logs for the job id instead
      of just the submission's logs */}
        {logsLink}
      </TableCell>
      <TableCell align="center">
        {driver_info && ipLogMap[driver_info.node_ip_address] && (
          <React.Fragment>
            <br />
            <a
              href={`/worker/traceback?pid=${driver_info.pid}&ip=${driver_info.node_ip_address}&native=0`}
              target="_blank"
              title="Sample the current Python stack trace for this worker."
              rel="noreferrer"
            >
              Stack&nbsp;Trace
            </a>
            <br />
            <a
              href={`/worker/cpu_profile?pid=${driver_info.pid}&ip=${driver_info.node_ip_address}&duration=5&native=0`}
              target="_blank"
              title="Profile the Python worker for 5 seconds (default) and display a CPU flame graph."
              rel="noreferrer"
            >
              CPU&nbsp;Flame&nbsp;Graph
            </a>
            <br />
            <a
              href={`/worker/memory_profile?pid=${driver_info.pid}&ip=${driver_info.node_ip_address}&duration=5&native=0`}
              target="_blank"
              title="Profile the Python worker for 5 seconds (default) and display a memory flame graph."
              rel="noreferrer"
            >
              Memory&nbsp;Flame&nbsp;Graph
            </a>
            <br />
          </React.Fragment>
        )}
      </TableCell>
      <TableCell align="center">
        {dayjs(Number(start_time)).format("YYYY/MM/DD HH:mm:ss")}
      </TableCell>
      <TableCell align="center">
        {end_time && end_time > 0
          ? dayjs(Number(end_time)).format("YYYY/MM/DD HH:mm:ss")
          : "-"}
      </TableCell>
      <TableCell align="center">{driver_info?.pid ?? "-"}</TableCell>
    </TableRow>
  );
};
