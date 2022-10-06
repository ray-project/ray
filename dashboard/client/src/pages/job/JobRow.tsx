import { TableCell, TableRow } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useContext } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../../App";
import { UnifiedJob } from "../../type/job";
import { useJobProgress } from "./hook/useJobProgress";
import { MiniTaskProgressBar } from "./TaskProgressBar";

type JobRowProps = {
  job: UnifiedJob;
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
  },
}: JobRowProps) => {
  const { ipLogMap } = useContext(GlobalContext);
  const { progress, error } = useJobProgress(job_id ?? undefined);

  const progressBar = (() => {
    if (!progress || error) {
      if (status === "SUCCEEDED" || status === "FAILED") {
        // Show a fake all-green progress bar.
        return <MiniTaskProgressBar numFinished={1} showTooltip={false} />;
      } else {
        return "unavailable";
      }
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
      <TableCell align="center">{job_id ?? "-"}</TableCell>
      <TableCell align="center">{submission_id ?? "-"}</TableCell>
      <TableCell align="center">{status}</TableCell>
      <TableCell align="center">{progressBar}</TableCell>
      <TableCell align="center">
        {/* TODO(aguo): Also show logs for the job id instead
      of just the submission's logs */}
        {driver_info && ipLogMap[driver_info.node_ip_address] ? (
          <Link
            to={`/log/${encodeURIComponent(
              ipLogMap[driver_info.node_ip_address],
            )}?fileName=${
              type === "DRIVER" ? job_id : `driver-${submission_id}`
            }`}
            target="_blank"
          >
            Log
          </Link>
        ) : (
          "-"
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
