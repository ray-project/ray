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
import Autocomplete from "@mui/material/Autocomplete";
import Pagination from "@mui/material/Pagination";
import React from "react";
import { Outlet } from "react-router-dom";
import { sliceToPage } from "../../common/util";
import Loading from "../../components/Loading";
import { SearchInput } from "../../components/SearchComponent";
import TitleCard from "../../components/TitleCard";
import { HelpInfo } from "../../components/Tooltip";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useJobList } from "./hook/useJobList";
import { JobRow } from "./JobRow";

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

  const {
    items: list,
    constrainedPage,
    maxPage,
  } = sliceToPage(jobList, page.pageNo, page.pageSize);

  return (
    <Box sx={{ padding: 2, width: "100%" }}>
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
            <SearchInput
              label="Submission ID"
              onChange={(value) => changeFilter("submission_id", value)}
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
            <Autocomplete
              sx={{ height: 35, width: 150 }}
              options={["PENDING", "RUNNING", "SUCCEEDED", "STOPPED", "FAILED"]}
              onInputChange={(event, value) =>
                changeFilter("status", value.trim())
              }
              renderInput={(params) => <TextField {...params} label="Status" />}
            />
          </Box>
          <div>
            <Pagination
              count={maxPage}
              page={constrainedPage}
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
                        <HelpInfo sx={{ marginLeft: 1 }}>{helpInfo}</HelpInfo>
                      )}
                    </Box>
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {list.map((job, index) => {
                const { job_id, submission_id } = job;
                return (
                  <JobRow key={job_id ?? submission_id ?? index} job={job} />
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
      </TitleCard>
    </Box>
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
