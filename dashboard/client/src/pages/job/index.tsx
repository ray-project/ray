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
import { SearchInput, SearchSelect } from "../../components/SearchComponent";
import TitleCard from "../../components/TitleCard";
import { useJobList } from "./hook/useJobList";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
  },
}));

const columns = ["ID", "DriverIpAddress", "DriverPid", "IsDead", "Timestamp"];

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

  return (
    <div className={classes.root}>
      <Loading loading={msg.startsWith("Loading")} />
      <TitleCard title="JOBS">
        Auto Refresh:
        <Switch
          checked={isRefreshing}
          onChange={onSwitchChange}
          name="refresh"
          inputProps={{ "aria-label": "secondary checkbox" }}
        />
        <br />
        Request Status: {msg}
      </TitleCard>
      <TitleCard title="Job List">
        <TableContainer>
          <SearchInput
            label="ID"
            onChange={(value) => changeFilter("jobId", value)}
          />
          <SearchSelect
            label="Language"
            onChange={(value) => changeFilter("language", value)}
            options={["JAVA", "PYTHON"]}
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
                  ({
                    jobId = "",
                    driverIpAddress,
                    isDead,
                    driverPid,
                    timestamp,
                  }) => (
                    <TableRow key={jobId}>
                      <TableCell align="center">
                        <Link to={`/job/${jobId}`}>{jobId}</Link>
                      </TableCell>
                      <TableCell align="center">{driverIpAddress}</TableCell>
                      <TableCell align="center">{driverPid}</TableCell>
                      <TableCell align="center">
                        {isDead ? "true" : "false"}
                      </TableCell>
                      <TableCell align="center">
                        {dayjs(timestamp * 1000).format("YYYY/MM/DD HH:mm:ss")}
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
