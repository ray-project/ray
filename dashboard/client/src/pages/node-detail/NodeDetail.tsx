import {
  Grid,
  makeStyles,
  Switch,
  Tab,
  TableContainer,
  Tabs,
} from "@material-ui/core";
import moment from "moment";
import numeral from "numeral";
import React from "react";
import { RouteComponentProps } from "react-router-dom";
import ActorTable from "../../common/ActorTable";
import Loading from "../../common/Loading";
import PercentageBar from "../../common/PercentageBar";
import { StatusChip } from "../../common/StatusChip";
import TitleCard from "../../common/TitleCard";
import RayletWorkerTable from "../../common/WorkerTable";
import { ViewMeasures } from "../../type/raylet";
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

const showMeasureKeys = [
  "local_total_resource",
  "local_available_resource",
  "actor_stats",
  "task_dependency_manager_stats",
  "reconstruction_policy_stats",
  "scheduling_queue_stats",
  "object_manager_stats",
];

const ViewDataDisplayer = ({ view }: { view?: ViewMeasures }) => {
  if (!view) {
    return null;
  }
  const { tags = "", ...otherProps } = view;

  return (
    <Grid item xs={6}>
      <span>{tags.split(",").pop()?.split(":").slice(1).join(":")}</span>=
      {Object.keys(otherProps).length > 0 ? (
        JSON.stringify(Object.values(otherProps).pop())
      ) : (
        <span style={{ color: "gray" }}>null</span>
      )}
    </Grid>
  );
};

const NodeDetailPage = (props: RouteComponentProps<{ id: string }>) => {
  const classes = useStyle();
  const {
    params,
    selectedTab,
    nodeDetail,
    msg,
    isRefreshing,
    onRefreshChange,
    raylet,
    handleChange,
  } = useNodeDetail(props);

  return (
    <div className={classes.root}>
      <Loading loading={msg.startsWith("Loading")} />
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
                <div className={classes.label}>CPU (Logic/Physic)</div>{" "}
                {nodeDetail.cpus[0]}/ {nodeDetail.cpus[1]}
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
                {moment(nodeDetail.bootTime * 1000).format(
                  "YYYY/MM/DD HH:mm:ss",
                )}
              </Grid>
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>Sent Tps</div>{" "}
                {numeral(nodeDetail?.net[0]).format("0.00b")}/s
              </Grid>
              <Grid item xs>
                <div className={classes.label}>Recieved Tps</div>{" "}
                {numeral(nodeDetail?.net[1]).format("0.00b")}/s
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
                    {numeral(nodeDetail?.mem[0] - nodeDetail?.mem[1]).format(
                      "0.00b",
                    )}
                    /{numeral(nodeDetail?.mem[0]).format("0.00b")}(
                    {nodeDetail?.mem[2]}%)
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
                        {numeral(obj.used).format("0.00b")}/
                        {numeral(obj.total).format("0.00b")}({obj.percent}%,{" "}
                        {numeral(obj.free).format("0.00b")} free)
                      </PercentageBar>
                    )}
                  </Grid>
                ))}
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
              {showMeasureKeys
                .map((e) => raylet.viewData.find((view) => view.viewName === e))
                .map((e) =>
                  e ? (
                    <React.Fragment key={e.viewName}>
                      <p className={classes.label}>
                        {e.viewName
                          .split("_")
                          .map((e) => e[0].toUpperCase() + e.slice(1))
                          .join(" ")}
                      </p>
                      <Grid
                        container
                        spacing={2}
                        style={{ maxHeight: 177, overflow: "auto" }}
                      >
                        {e.measures.map((e) => (
                          <ViewDataDisplayer key={e.tags} view={e} />
                        ))}
                      </Grid>
                    </React.Fragment>
                  ) : null,
                )}
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
              />
            </TableContainer>
          </React.Fragment>
        )}
      </TitleCard>
    </div>
  );
};

export default NodeDetailPage;
