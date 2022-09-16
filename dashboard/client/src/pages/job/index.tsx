import {
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import Pagination from "@material-ui/lab/Pagination";
import dayjs from "dayjs";
import React from "react";
import { Link } from "react-router-dom";
import Loading from "../../components/Loading";
import { ProgressBar } from "../../components/ProgressBar";
import { SearchInput } from "../../components/SearchComponent";
import TitleCard from "../../components/TitleCard";
import { useJobList } from "./hook/useJobList";
import { useJobProgress } from "./hook/useJobProgress";
import { TaskProgressBar } from "./TaskProgressBar";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
  },
}));

const columns = [
  "Job ID",
  "Submission ID",
  "Status",
  "Logs",
  "StartTime",
  "EndTime",
  "Driver Pid",
];

const JobList = () => {
  const classes = useStyles();
  const {
    msg,
    isRefreshing,
    onSwitchChange,
    jobList,
    changeFilter,
    page,
    setPage,
    ipLogMap,
  } = useJobList();

  const { progress, onSwitchChange: onProgressSwitchChange } = useJobProgress();

  return (
    <div className={classes.root}>
      <Loading loading={msg.startsWith("Loading")} />
      <TitleCard title="JOBS">
        Auto Refresh:
        <Switch
          checked={isRefreshing}
          onChange={(event) => {
            onSwitchChange(event);
            onProgressSwitchChange(event);
          }}
          name="refresh"
          inputProps={{ "aria-label": "secondary checkbox" }}
        />
        <br />
        Request Status: {msg}
      </TitleCard>
      <TitleCard title="Progress">
        <TaskProgressBar {...progress} />
      </TitleCard>
      <TitleCard title="Job List">
        <TableContainer>
          <SearchInput
            label="Job ID"
            onChange={(value) => changeFilter("job_id", value)}
          />
          <SearchInput
            label="Page Size"
            onChange={(value) =>
              setPage("pageSize", Math.min(Number(value), 500) || 10)
            }
          />
          <div>
            <Pagination
              count={Math.ceil(jobList.length / page.pageSize)}
              page={page.pageNo}
              onChange={(e, pageNo) => setPage("pageNo", pageNo)}
            />
          </div>
          <Table>
            <TableHead>
              <TableRow>
                {columns.map((col) => (
                  <TableCell align="center" key={col}>
                    {col}
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {jobList
                .slice(
                  (page.pageNo - 1) * page.pageSize,
                  page.pageNo * page.pageSize,
                )
                .map(
                  (
                    {
                      job_id,
                      submission_id,
                      driver_info,
                      type,
                      status,
                      start_time,
                      end_time,
                    },
                    index,
                  ) => (
                    <TableRow key={job_id ?? submission_id ?? index}>
                      <TableCell align="center">{job_id ?? "-"}</TableCell>
                      <TableCell align="center">
                        {submission_id ?? "-"}
                      </TableCell>
                      <TableCell align="center">{status}</TableCell>
                      <TableCell align="center">
                        {/* TODO(aguo): Also show logs for the job id instead
                        of just the submission's logs */}
                        {driver_info &&
                        ipLogMap[driver_info.node_ip_address] ? (
                          <Link
                            to={`/log/${encodeURIComponent(
                              ipLogMap[driver_info.node_ip_address],
                            )}?fileName=${
                              type === "DRIVER"
                                ? job_id
                                : `driver-${submission_id}`
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
                        {dayjs(Number(start_time)).format(
                          "YYYY/MM/DD HH:mm:ss",
                        )}
                      </TableCell>
                      <TableCell align="center">
                        {end_time && end_time > 0
                          ? dayjs(Number(end_time)).format(
                              "YYYY/MM/DD HH:mm:ss",
                            )
                          : "-"}
                      </TableCell>
                      <TableCell align="center">
                        {driver_info?.pid ?? "-"}
                      </TableCell>
                    </TableRow>
                  ),
                )}
            </TableBody>
          </Table>
        </TableContainer>
      </TitleCard>
    </div>
  );
};

export default JobList;
