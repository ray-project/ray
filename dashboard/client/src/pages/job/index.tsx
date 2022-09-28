import {
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import Pagination from "@material-ui/lab/Pagination";
import React from "react";
import Loading from "../../components/Loading";
import { SearchInput } from "../../components/SearchComponent";
import TitleCard from "../../components/TitleCard";
import { useJobList } from "./hook/useJobList";
import { useJobProgress } from "./hook/useJobProgress";
import { JobRow } from "./JobRow";
import { TaskProgressBar } from "./TaskProgressBar";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
  },
  progressError: {
    marginTop: theme.spacing(1),
  },
}));

const columns = [
  "Job ID",
  "Submission ID",
  "Status",
  "Progress",
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
  } = useJobList();

  const {
    progress,
    error: progressError,
    msg: progressMsg,
    onSwitchChange: onProgressSwitchChange,
  } = useJobProgress();

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
        {progressError && (
          <Typography className={classes.progressError} color="error">
            {progressMsg}
          </Typography>
        )}
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
                .map((job, index) => {
                  const { job_id, submission_id } = job;
                  return (
                    <JobRow key={job_id ?? submission_id ?? index} job={job} />
                  );
                })}
            </TableBody>
          </Table>
        </TableContainer>
      </TitleCard>
    </div>
  );
};

export default JobList;
