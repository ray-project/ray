import {
  Grid,
  makeStyles,
  Switch,
  Tab,
  TableContainer,
  Tabs,
} from "@material-ui/core";
import React from "react";
import { Link } from "react-router-dom";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import ActorTable from "../../components/ActorTable";
import Loading from "../../components/Loading";
import PercentageBar from "../../components/PercentageBar";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import RayletWorkerTable from "../../components/WorkerTable";
import { memoryConverter } from "../../util/converter";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useNodeDetail } from "./hook/useNodeDetail";

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
  tab: {
    marginBottom: theme.spacing(2),
  },
}));

const NodeDetailPage = () => {
  const classes = useStyle();
  const {
    params,
    selectedTab,
    nodeDetail,
    msg,
    isLoading,
    isRefreshing,
    onRefreshChange,
    raylet,
    handleChange,
  } = useNodeDetail();

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={{
          title: `Node: ${params.id}`,
          pageTitle: `${params.id} | Node`,
          id: "node-detail",
          path: `/cluster/nodes/${params.id}`,
        }}
      />
      <Loading loading={isLoading} />
      <TitleCard title={`NODE - ${params.id}`}>
        <StatusChip
          type="node"
          status={nodeDetail?.raylet?.state || "LOADING"}
        />
        <br />
        Auto Refresh:
        <Switch
          checked={isRefreshing}
          onChange={onRefreshChange}
          name="refresh"
          inputProps={{ "aria-label": "secondary checkbox" }}
        />
        <br />
        Request Status: {msg}
      </TitleCard>
      <TitleCard title="Node Detail">
        <Tabs
          value={selectedTab}
          onChange={handleChange}
          className={classes.tab}
        >
          <Tab value="info" label="Info" />
          <Tab value="raylet" label="Raylet" />
          <Tab
            value="worker"
            label={`Worker (${nodeDetail?.workers.length || 0})`}
          />
          <Tab
            value="actor"
            label={`Actor (${
              Object.values(nodeDetail?.actors || {}).length || 0
            })`}
          />
        </Tabs>
        {nodeDetail && selectedTab === "info" && (
          <div className={classes.paper}>
            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>Hostname</div>{" "}
                {nodeDetail.hostname}
              </Grid>
              <Grid item xs>
                <div className={classes.label}>IP</div> {nodeDetail.ip}
              </Grid>
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs>
                {nodeDetail.cpus && (
                  <React.Fragment>
                    <div className={classes.label}>CPU (Logic/Physic)</div>{" "}
                    {nodeDetail.cpus[0]}/ {nodeDetail.cpus[1]}
                  </React.Fragment>
                )}
              </Grid>
              <Grid item xs>
                <div className={classes.label}>Load (1/5/15min)</div>{" "}
                {nodeDetail?.loadAvg[0] &&
                  nodeDetail.loadAvg[0]
                    .map((e) => Number(e).toFixed(2))
                    .join("/")}
              </Grid>
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>Load per CPU (1/5/15min)</div>{" "}
                {nodeDetail?.loadAvg[1] &&
                  nodeDetail.loadAvg[1]
                    .map((e) => Number(e).toFixed(2))
                    .join("/")}
              </Grid>
              <Grid item xs>
                <div className={classes.label}>Boot Time</div>{" "}
                {formatDateFromTimeMs(nodeDetail.bootTime * 1000)}
              </Grid>
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>Sent Tps</div>{" "}
                {memoryConverter(nodeDetail?.networkSpeed[0])}/s
              </Grid>
              <Grid item xs>
                <div className={classes.label}>Recieved Tps</div>{" "}
                {memoryConverter(nodeDetail?.networkSpeed[1])}/s
              </Grid>
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>Memory</div>{" "}
                {nodeDetail?.mem && (
                  <PercentageBar
                    num={Number(nodeDetail?.mem[0] - nodeDetail?.mem[1])}
                    total={nodeDetail?.mem[0]}
                  >
                    {memoryConverter(nodeDetail?.mem[0] - nodeDetail?.mem[1])}/
                    {memoryConverter(nodeDetail?.mem[0])}({nodeDetail?.mem[2]}%)
                  </PercentageBar>
                )}
              </Grid>
              <Grid item xs>
                <div className={classes.label}>CPU</div>{" "}
                <PercentageBar num={Number(nodeDetail.cpu)} total={100}>
                  {nodeDetail.cpu}%
                </PercentageBar>
              </Grid>
            </Grid>
            <Grid container spacing={2}>
              {nodeDetail?.disk &&
                Object.entries(nodeDetail?.disk).map(([path, obj]) => (
                  <Grid item xs={6} key={path}>
                    <div className={classes.label}>Disk ({path})</div>{" "}
                    {obj && (
                      <PercentageBar num={Number(obj.used)} total={obj.total}>
                        {memoryConverter(obj.used)}/{memoryConverter(obj.total)}
                        ({obj.percent}%, {memoryConverter(obj.free)} free)
                      </PercentageBar>
                    )}
                  </Grid>
                ))}
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>Logs</div>{" "}
                <Link
                  to={`/logs/?nodeId=${encodeURIComponent(
                    nodeDetail.raylet.nodeId,
                  )}`}
                >
                  log
                </Link>
              </Grid>
            </Grid>
          </div>
        )}
        {raylet && Object.keys(raylet).length > 0 && selectedTab === "raylet" && (
          <React.Fragment>
            <div className={classes.paper}>
              <Grid container spacing={2}>
                <Grid item xs>
                  <div className={classes.label}>Command</div>
                  <br />
                  <div style={{ height: 200, overflow: "auto" }}>
                    {nodeDetail?.cmdline.join(" ")}
                  </div>
                </Grid>
              </Grid>
              <Grid container spacing={2}>
                <Grid item xs>
                  <div className={classes.label}>Pid</div> {raylet?.pid}
                </Grid>
                <Grid item xs>
                  <div className={classes.label}>Workers Num</div>{" "}
                  {raylet?.numWorkers}
                </Grid>
                <Grid item xs>
                  <div className={classes.label}>Node Manager Port</div>{" "}
                  {raylet?.nodeManagerPort}
                </Grid>
              </Grid>
            </div>
          </React.Fragment>
        )}
        {nodeDetail?.workers && selectedTab === "worker" && (
          <React.Fragment>
            <TableContainer className={classes.paper}>
              <RayletWorkerTable
                workers={nodeDetail?.workers}
                actorMap={nodeDetail?.actors}
              />
            </TableContainer>
          </React.Fragment>
        )}
        {nodeDetail?.actors && selectedTab === "actor" && (
          <React.Fragment>
            <TableContainer className={classes.paper}>
              <ActorTable
                actors={nodeDetail.actors}
                workers={nodeDetail?.workers}
                detailPathPrefix="/actors"
              />
            </TableContainer>
          </React.Fragment>
        )}
      </TitleCard>
    </div>
  );
};

export default NodeDetailPage;
