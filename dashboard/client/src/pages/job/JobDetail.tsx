import {
  Grid,
  makeStyles,
  Switch,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tabs,
} from "@material-ui/core";
import React from "react";
import { Link, RouteComponentProps } from "react-router-dom";
import ActorTable from "../../components/ActorTable";
import Loading from "../../components/Loading";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import RayletWorkerTable from "../../components/WorkerTable";
import { longTextCut } from "../../util/func";
import { useJobDetail } from "./hook/useJobDetail";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
  paper: {
    padding: theme.spacing(2),
    marginTop: theme.spacing(2),
    marginBottom: theme.spacing(2),
  },
  label: {
    fontWeight: "bold",
  },
  pageMeta: {
    padding: theme.spacing(2),
    marginTop: theme.spacing(2),
  },
  tab: {
    marginBottom: theme.spacing(2),
  },
  dependenciesChip: {
    margin: theme.spacing(0.5),
    wordBreak: "break-all",
  },
  alert: {
    color: theme.palette.error.main,
  },
}));

const JobDetailPage = (props: RouteComponentProps<{ id: string }>) => {
  const classes = useStyle();
  const {
    actorMap,
    jobInfo,
    job,
    msg,
    selectedTab,
    handleChange,
    handleSwitchChange,
    params,
    refreshing,
    ipLogMap,
  } = useJobDetail(props);

  if (!job || !jobInfo) {
    return (
      <div className={classes.root}>
        <Loading loading={msg.startsWith("Loading")} />
        <TitleCard title={`JOB - ${params.id}`}>
          <StatusChip type="job" status="LOADING" />
          <br />
          Auto Refresh:
          <Switch
            checked={refreshing}
            onChange={handleSwitchChange}
            name="refresh"
            inputProps={{ "aria-label": "secondary checkbox" }}
          />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </div>
    );
  }

  return (
    <div className={classes.root}>
      <TitleCard title={`JOB - ${params.id}`}>
        <StatusChip type="job" status={jobInfo.isDead ? "DEAD" : "ALIVE"} />
        <br />
        Auto Refresh:
        <Switch
          checked={refreshing}
          onChange={handleSwitchChange}
          name="refresh"
          inputProps={{ "aria-label": "secondary checkbox" }}
        />
        <br />
        Request Status: {msg} <br />
      </TitleCard>
      <TitleCard title="Job Detail">
        <Tabs
          value={selectedTab}
          onChange={handleChange}
          className={classes.tab}
        >
          <Tab value="info" label="Info" />
          <Tab value="dep" label="Dependencies" />
          <Tab
            value="worker"
            label={`Worker(${job?.jobWorkers?.length || 0})`}
          />
          <Tab
            value="actor"
            label={`Actor(${Object.entries(job?.jobActors || {}).length || 0})`}
          />
        </Tabs>
        {selectedTab === "info" && (
          <Grid container spacing={2}>
            <Grid item xs={4}>
              <span className={classes.label}>Driver IP</span>:{" "}
              {jobInfo.driverIpAddress}
            </Grid>
            {ipLogMap[jobInfo.driverIpAddress] && (
              <Grid item xs={4}>
                <span className={classes.label}>Driver Log</span>:{" "}
                <Link
                  to={`/log/${encodeURIComponent(
                    ipLogMap[jobInfo.driverIpAddress],
                  )}?fileName=driver-${jobInfo.jobId}`}
                  target="_blank"
                >
                  Log
                </Link>
              </Grid>
            )}
            <Grid item xs={4}>
              <span className={classes.label}>Driver Pid</span>:{" "}
              {jobInfo.driverPid}
            </Grid>
            {jobInfo.eventUrl && (
              <Grid item xs={4}>
                <span className={classes.label}>Event Link</span>:{" "}
                <a
                  href={jobInfo.eventUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Event Log
                </a>
              </Grid>
            )}
            {jobInfo.failErrorMessage && (
              <Grid item xs={12}>
                <span className={classes.label}>Fail Error</span>:{" "}
                <span className={classes.alert}>
                  {jobInfo.failErrorMessage}
                </span>
              </Grid>
            )}
          </Grid>
        )}
        {jobInfo?.dependencies && selectedTab === "dep" && (
          <div className={classes.paper}>
            {jobInfo?.dependencies?.python && (
              <TitleCard title="Python Dependencies">
                <div
                  style={{
                    display: "flex",
                    justifyItems: "space-around",
                    flexWrap: "wrap",
                  }}
                >
                  {jobInfo.dependencies.python.map((e) => (
                    <StatusChip
                      type="deps"
                      status={e.startsWith("http") ? longTextCut(e, 30) : e}
                      key={e}
                    />
                  ))}
                </div>
              </TitleCard>
            )}
            {jobInfo?.dependencies?.java && (
              <TitleCard title="Java Dependencies">
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        {["Name", "Version", "URL"].map((col) => (
                          <TableCell align="center" key={col}>
                            {col}
                          </TableCell>
                        ))}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {jobInfo.dependencies.java.map(
                        ({ name, version, url }) => (
                          <TableRow key={url}>
                            <TableCell align="center">{name}</TableCell>
                            <TableCell align="center">{version}</TableCell>
                            <TableCell align="center">
                              <a
                                href={url}
                                target="_blank"
                                rel="noopener noreferrer"
                              >
                                {url}
                              </a>
                            </TableCell>
                          </TableRow>
                        ),
                      )}
                    </TableBody>
                  </Table>
                </TableContainer>
              </TitleCard>
            )}
          </div>
        )}
        {selectedTab === "worker" && (
          <div>
            <TableContainer className={classes.paper}>
              <RayletWorkerTable
                workers={job.jobWorkers}
                actorMap={actorMap || {}}
              />
            </TableContainer>
          </div>
        )}
        {selectedTab === "actor" && (
          <div>
            <TableContainer className={classes.paper}>
              <ActorTable actors={actorMap || {}} workers={job.jobWorkers} />
            </TableContainer>
          </div>
        )}
      </TitleCard>
    </div>
  );
};

export default JobDetailPage;
