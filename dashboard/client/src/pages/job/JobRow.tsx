import { TableCell, TableRow, Tooltip } from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import React from "react";
import { Link } from "react-router-dom";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { JobStatusWithIcon } from "../../common/JobStatus";
import {
  CpuProfilingLink,
  CpuStackTraceLink,
} from "../../common/ProfilingLink";
import { UnifiedJob } from "../../type/job";
import { useJobProgress } from "./hook/useJobProgress";
import { JobLogsLink } from "./JobDetail";
import { MiniTaskProgressBar } from "./TaskProgressBar";

const useStyles = makeStyles((theme) => ({
  overflowCell: {
    display: "block",
    margin: "auto",
    maxWidth: 360,
    textOverflow: "ellipsis",
    overflow: "hidden",
    whiteSpace: "nowrap",
  },
}));

type JobRowProps = {
  job: UnifiedJob;
};

export const JobRow = ({ job }: JobRowProps) => {
  const {
    job_id,
    submission_id,
    driver_info,
    status,
    start_time,
    end_time,
    entrypoint,
  } = job;
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

  return (
    <TableRow>
      <TableCell align="center">
        {job_id ? <Link to={`${job_id}`}>{job_id}</Link> : "-"}
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
      <TableCell align="center">
        <JobStatusWithIcon job={job} />
      </TableCell>
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
        <JobLogsLink job={job} />
        <br />
        <CpuProfilingLink
          pid={job.driver_info?.pid}
          ip={job.driver_info?.node_ip_address}
          type="Driver"
        />
        <br />
        <CpuStackTraceLink
          pid={job.driver_info?.pid}
          ip={job.driver_info?.node_ip_address}
          type="Driver"
        />
      </TableCell>
      <TableCell align="center">
        {start_time ? formatDateFromTimeMs(start_time) : "-"}
      </TableCell>
      <TableCell align="center">
        {end_time && end_time > 0 ? formatDateFromTimeMs(end_time) : "-"}
      </TableCell>
      <TableCell align="center">{driver_info?.pid ?? "-"}</TableCell>
    </TableRow>
  );
};
