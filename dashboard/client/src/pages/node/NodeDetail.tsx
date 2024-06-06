import { Grid, Switch, Tab, TableContainer, Tabs } from "@mui/material";
import { styled } from "@mui/material/styles";
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

const RootDiv = styled("div")(({theme}) => ({
  padding: theme.spacing(2),
}));

const PaperDiv = styled("div")(({theme}) => ({
  padding: theme.spacing(2),
  marginTop: theme.spacing(2),
  marginBottom: theme.spacing(2),
}));

const PaperTableContainer = styled(TableContainer)(({theme}) => ({
  padding: theme.spacing(2),
  marginTop: theme.spacing(2),
  marginBottom: theme.spacing(2),
}));

const LabelDiv = styled("div")(({theme}) => ({
  fontWeight: "bold",
}));

const StyledTabs = styled(Tabs)(({theme}) => ({
  marginBottom: theme.spacing(2),
}));

const NodeDetailPage = () => {
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
    <RootDiv>
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
        <StyledTabs value={selectedTab} onChange={handleChange}>
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
        </StyledTabs>
        {nodeDetail && selectedTab === "info" && (
          <PaperDiv>
            <Grid container spacing={2}>
              <Grid item xs>
                <LabelDiv>Hostname</LabelDiv>{" "}
                {nodeDetail.hostname}
              </Grid>
              <Grid item xs>
                <LabelDiv>IP</LabelDiv> {nodeDetail.ip}
              </Grid>
            </Grid>
            <Grid container>
              <Grid item xs>
                {nodeDetail.cpus && (
                  <React.Fragment>
                    <LabelDiv>CPU (Logic/Physic)</LabelDiv>{" "}
                    {nodeDetail.cpus[0]}/ {nodeDetail.cpus[1]}
                  </React.Fragment>
                )}
              </Grid>
              <Grid item xs>
                <LabelDiv>Load (1/5/15min)</LabelDiv>{" "}
                {nodeDetail?.loadAvg[0] &&
                  nodeDetail.loadAvg[0]
                    .map((e) => Number(e).toFixed(2))
                    .join("/")}
              </Grid>
            </Grid>
            <Grid container>
              <Grid item xs>
                <LabelDiv>Load per CPU (1/5/15min)</LabelDiv>{" "}
                {nodeDetail?.loadAvg[1] &&
                  nodeDetail.loadAvg[1]
                    .map((e) => Number(e).toFixed(2))
                    .join("/")}
              </Grid>
              <Grid item xs>
                <LabelDiv>Boot Time</LabelDiv>{" "}
                {formatDateFromTimeMs(nodeDetail.bootTime * 1000)}
              </Grid>
            </Grid>
            <Grid container>
              <Grid item xs>
                <LabelDiv>Sent Tps</LabelDiv>{" "}
                {memoryConverter(nodeDetail?.networkSpeed[0])}/s
              </Grid>
              <Grid item xs>
                <LabelDiv>Recieved Tps</LabelDiv>{" "}
                {memoryConverter(nodeDetail?.networkSpeed[1])}/s
              </Grid>
            </Grid>
            <Grid container>
              <Grid item xs>
                <LabelDiv>Memory</LabelDiv>{" "}
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
                <LabelDiv>CPU</LabelDiv>{" "}
                <PercentageBar num={Number(nodeDetail.cpu)} total={100}>
                  {nodeDetail.cpu}%
                </PercentageBar>
              </Grid>
            </Grid>
            <Grid container>
              {nodeDetail?.disk &&
                Object.entries(nodeDetail?.disk).map(([path, obj]) => (
                  <Grid item xs={6} key={path}>
                    <LabelDiv>Disk ({path})</LabelDiv>{" "}
                    {obj && (
                      <PercentageBar num={Number(obj.used)} total={obj.total}>
                        {memoryConverter(obj.used)}/{memoryConverter(obj.total)}
                        ({obj.percent}%, {memoryConverter(obj.free)} free)
                      </PercentageBar>
                    )}
                  </Grid>
                ))}
            </Grid>
            <Grid container>
              <Grid item xs>
                <LabelDiv>Logs</LabelDiv>{" "}
                <Link
                  to={`/logs/?nodeId=${encodeURIComponent(
                    nodeDetail.raylet.nodeId,
                  )}`}
                >
                  log
                </Link>
              </Grid>
            </Grid>
          </PaperDiv>
        )}
        {raylet && Object.keys(raylet).length > 0 && selectedTab === "raylet" && (
          <React.Fragment>
            <PaperDiv>
              <Grid container spacing={2}>
                <Grid item xs>
                  <LabelDiv>Command</LabelDiv>
                  <br />
                  <div style={{ height: 200, overflow: "auto" }}>
                    {nodeDetail?.cmdline.join(" ")}
                  </div>
                </Grid>
              </Grid>
              <Grid container>
                <Grid item xs>
                  <LabelDiv>Pid</LabelDiv> {raylet?.pid}
                </Grid>
                <Grid item xs>
                  <LabelDiv>Workers Num</LabelDiv>{" "}
                  {raylet?.numWorkers}
                </Grid>
                <Grid item xs>
                  <LabelDiv>Node Manager Port</LabelDiv>{" "}
                  {raylet?.nodeManagerPort}
                </Grid>
              </Grid>
            </PaperDiv>
          </React.Fragment>
        )}
        {nodeDetail?.workers && selectedTab === "worker" && (
          <React.Fragment>
            <PaperTableContainer>
              <RayletWorkerTable
                workers={nodeDetail?.workers}
                actorMap={nodeDetail?.actors}
              />
            </PaperTableContainer>
          </React.Fragment>
        )}
        {nodeDetail?.actors && selectedTab === "actor" && (
          <React.Fragment>
            <PaperTableContainer>
              <ActorTable
                actors={nodeDetail.actors}
                workers={nodeDetail?.workers}
                detailPathPrefix="/actors"
              />
            </PaperTableContainer>
          </React.Fragment>
        )}
      </TitleCard>
    </RootDiv>
  );
};

export default NodeDetailPage;
