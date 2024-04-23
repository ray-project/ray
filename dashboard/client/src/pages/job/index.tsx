import {
  Box,
  InputAdornment,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from "@mui/material";
import Pagination from "@mui/material/Pagination";
import makeStyles from "@mui/styles/makeStyles";
import React from "react";
import { Outlet } from "react-router-dom";
import Loading from "../../components/Loading";
import { SearchInput } from "../../components/SearchComponent";
import TitleCard from "../../components/TitleCard";
import { HelpInfo } from "../../components/Tooltip";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useJobList } from "./hook/useJobList";
import { JobRow } from "./JobRow";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
  },
  progressError: {
    marginTop: theme.spacing(1),
  },
  helpInfo: {
    marginLeft: theme.spacing(1),
  },
}));

const columns = [
  { label: "Job ID" },
  { label: "Submission ID" },
  { label: "Entrypoint" },
  { label: "Status" },
  { label: "Status message" },
  { label: "Duration" },
  {
    label: "Tasks",
    helpInfo: (
      <Typography>
        The progress of the all submitted tasks per job. Tasks that are not yet
        submitted do not show up in the progress bar.
      </Typography>
    ),
  },
  {
    label: "Actions",
  },
  { label: "StartTime" },
  { label: "EndTime" },
  { label: "Driver Pid" },
];

const JobList = () => {
  const classes = useStyles();
  const {
    msg,
    isLoading,
    isRefreshing,
    onSwitchChange,
    jobList,
    changeFilter,
    page,
    setPage,
  } = useJobList();

  return (
    <div className={classes.root}>
      <Loading loading={isLoading} />
      <TitleCard title="JOBS">
        Auto Refresh:
        <Switch
          checked={isRefreshing}
          onChange={(event) => {
            onSwitchChange(event);
          }}
          name="refresh"
          inputProps={{ "aria-label": "secondary checkbox" }}
        />
        <br />
        Request Status: {msg}
      </TitleCard>
      <TitleCard title="Job List">
        <TableContainer>
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              gap: 2,
              paddingTop: 1,
            }}
          >
            <SearchInput
              label="Job ID"
              onChange={(value) => changeFilter("job_id", value)}
            />
            <TextField
              sx={{ width: 120 }}
              label="Page Size"
              size="small"
              defaultValue={10}
              InputProps={{
                onChange: ({ target: { value } }) => {
                  setPage("pageSize", Math.min(Number(value), 500) || 10);
                },
                endAdornment: (
                  <InputAdornment position="end">Per Page</InputAdornment>
                ),
              }}
            />
          </Box>
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
                {columns.map(({ label, helpInfo }) => (
                  <TableCell align="center" key={label}>
                    <Box
                      display="flex"
                      justifyContent="center"
                      alignItems="center"
                    >
                      {label}
                      {helpInfo && (
                        <HelpInfo className={classes.helpInfo}>
                          {helpInfo}
                        </HelpInfo>
                      )}
                    </Box>
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

/**
 * Jobs page for the new information hierarchy
 */
export const JobsLayout = () => {
  return (
    <React.Fragment>
      <MainNavPageInfo
        pageInfo={{
          title: "Jobs",
          id: "jobs",
          path: "/jobs",
        }}
      />
      <Outlet />
    </React.Fragment>
  );
};

export default JobList;
