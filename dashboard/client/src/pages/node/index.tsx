import {
  Button,
  ButtonGroup,
  Grid,
  Paper,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
} from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import Pagination from "@material-ui/lab/Pagination";
import React from "react";
import { Link } from "react-router-dom";
import Loading from "../../components/Loading";
import PercentageBar from "../../components/PercentageBar";
import { SearchInput, SearchSelect } from "../../components/SearchComponent";
import StateCounter from "../../components/StatesCounter";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { NodeDetail } from "../../type/node";
import { memoryConverter } from "../../util/converter";
import { useNodeList } from "./hook/useNodeList";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
    position: "relative",
  },
}));

const columns = [
  "State",
  "ID",
  "Host",
  "IP",
  "CPU Usage",
  "Memory",
  "Disk(root)",
  "Sent",
  "Received",
  "Log",
];

export const brpcLinkChanger = (href: string) => {
  const { location } = window;
  const { pathname } = location;
  const pathArr = pathname.split("/");
  if (pathArr.some((e) => e.split(".").length > 1)) {
    const index = pathArr.findIndex((e) => e.includes("."));
    const resultArr = pathArr.slice(0, index);
    resultArr.push(href);
    return `${location.protocol}//${location.host}${resultArr.join("/")}`;
  }

  return `http://${href}`;
};

export const NodeCard = (props: { node: NodeDetail }) => {
  const { node } = props;

  if (!node) {
    return null;
  }

  const { raylet, hostname, ip, cpu, mem, networkSpeed, disk, logUrl } = node;
  const { nodeId, state } = raylet;

  return (
    <Paper variant="outlined" style={{ padding: "12px 12px", margin: 12 }}>
      <p style={{ fontWeight: "bold", fontSize: 12, textDecoration: "none" }}>
        <Link to={`node/${nodeId}`}>{nodeId}</Link>{" "}
      </p>
      <p>
        <Grid container spacing={1}>
          <Grid item>
            <StatusChip type="node" status={state} />
          </Grid>
          <Grid item>
            {hostname}({ip})
          </Grid>
          {networkSpeed && networkSpeed[0] >= 0 && (
            <Grid item>
              <span style={{ fontWeight: "bold" }}>Sent</span>{" "}
              {memoryConverter(networkSpeed[0])}/s{" "}
              <span style={{ fontWeight: "bold" }}>Received</span>{" "}
              {memoryConverter(networkSpeed[1])}/s
            </Grid>
          )}
        </Grid>
      </p>
      <Grid container spacing={1} alignItems="baseline">
        {cpu >= 0 && (
          <Grid item xs>
            CPU
            <PercentageBar num={Number(cpu)} total={100}>
              {cpu}%
            </PercentageBar>
          </Grid>
        )}
        {mem && (
          <Grid item xs>
            Memory
            <PercentageBar num={Number(mem[0] - mem[1])} total={mem[0]}>
              {memoryConverter(mem[0] - mem[1])}/{memoryConverter(mem[0])}(
              {mem[2]}%)
            </PercentageBar>
          </Grid>
        )}
        {disk && disk["/"] && (
          <Grid item xs>
            Disk('/')
            <PercentageBar num={Number(disk["/"].used)} total={disk["/"].total}>
              {memoryConverter(disk["/"].used)}/
              {memoryConverter(disk["/"].total)}({disk["/"].percent}%)
            </PercentageBar>
          </Grid>
        )}
      </Grid>
      <Grid container justify="flex-end" spacing={1} style={{ margin: 8 }}>
        <Grid>
          <Button>
            <Link to={`/log/${encodeURIComponent(logUrl)}`}>log</Link>
          </Button>
        </Grid>
      </Grid>
    </Paper>
  );
};

const Nodes = () => {
  const classes = useStyles();
  const {
    msg,
    isRefreshing,
    onSwitchChange,
    nodeList,
    changeFilter,
    page,
    setPage,
    setSortKey,
    setOrderDesc,
    mode,
    setMode,
  } = useNodeList();

  return (
    <div className={classes.root}>
      <Loading loading={msg.startsWith("Loading")} />
      <TitleCard title="NODES">
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
      <TitleCard title="Statistics">
        <StateCounter type="node" list={nodeList} />
      </TitleCard>
      <TitleCard title="Node List">
        <Grid container alignItems="center">
          <Grid item>
            <SearchInput
              label="Host"
              onChange={(value) => changeFilter("hostname", value.trim())}
            />
          </Grid>
          <Grid item>
            <SearchInput
              label="IP"
              onChange={(value) => changeFilter("ip", value.trim())}
            />
          </Grid>
          <Grid item>
            <SearchSelect
              label="State"
              onChange={(value) => changeFilter("state", value.trim())}
              options={["ALIVE", "DEAD"]}
            />
          </Grid>
          <Grid item>
            <SearchInput
              label="Page Size"
              onChange={(value) =>
                setPage("pageSize", Math.min(Number(value), 500) || 10)
              }
            />
          </Grid>
          <Grid item>
            <SearchSelect
              label="Sort By"
              options={[
                ["state", "State"],
                ["mem[2]", "Used Memory"],
                ["mem[0]", "Total Memory"],
                ["cpu", "CPU"],
                ["networkSpeed[0]", "Sent"],
                ["networkSpeed[1]", "Received"],
                ["disk./.used", "Used Disk"],
              ]}
              onChange={(val) => setSortKey(val)}
            />
          </Grid>
          <Grid item>
            <span style={{ margin: 8, marginTop: 0 }}>
              Reverse:
              <Switch onChange={(_, checked) => setOrderDesc(checked)} />
            </span>
          </Grid>
          <Grid item>
            <ButtonGroup size="small">
              <Button
                onClick={() => setMode("table")}
                color={mode === "table" ? "primary" : "default"}
              >
                Table
              </Button>
              <Button
                onClick={() => setMode("card")}
                color={mode === "card" ? "primary" : "default"}
              >
                Card
              </Button>
            </ButtonGroup>
          </Grid>
        </Grid>
        <div>
          <Pagination
            count={Math.ceil(nodeList.length / page.pageSize)}
            page={page.pageNo}
            onChange={(e, pageNo) => setPage("pageNo", pageNo)}
          />
        </div>
        {mode === "table" && (
          <TableContainer>
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
                {nodeList
                  .slice(
                    (page.pageNo - 1) * page.pageSize,
                    page.pageNo * page.pageSize,
                  )
                  .map(
                    (
                      {
                        hostname = "",
                        ip = "",
                        cpu = 0,
                        mem = [],
                        disk,
                        networkSpeed = [0, 0],
                        raylet,
                        logUrl,
                      }: NodeDetail,
                      i,
                    ) => (
                      <TableRow key={hostname + i}>
                        <TableCell>
                          <StatusChip type="node" status={raylet.state} />
                        </TableCell>
                        <TableCell align="center">
                          <Tooltip title={raylet.nodeId} arrow interactive>
                            <Link to={`/node/${raylet.nodeId}`}>
                              {raylet.nodeId.slice(0, 5)}
                            </Link>
                          </Tooltip>
                        </TableCell>
                        <TableCell align="center">{hostname}</TableCell>
                        <TableCell align="center">{ip}</TableCell>
                        <TableCell>
                          <PercentageBar num={Number(cpu)} total={100}>
                            {cpu}%
                          </PercentageBar>
                        </TableCell>
                        <TableCell>
                          <PercentageBar
                            num={Number(mem[0] - mem[1])}
                            total={mem[0]}
                          >
                            {memoryConverter(mem[0] - mem[1])}/
                            {memoryConverter(mem[0])}({mem[2]}%)
                          </PercentageBar>
                        </TableCell>
                        <TableCell>
                          {disk && disk["/"] && (
                            <PercentageBar
                              num={Number(disk["/"].used)}
                              total={disk["/"].total}
                            >
                              {memoryConverter(disk["/"].used)}/
                              {memoryConverter(disk["/"].total)}(
                              {disk["/"].percent}%)
                            </PercentageBar>
                          )}
                        </TableCell>
                        <TableCell align="center">
                          {memoryConverter(networkSpeed[0])}/s
                        </TableCell>
                        <TableCell align="center">
                          {memoryConverter(networkSpeed[1])}/s
                        </TableCell>
                        <TableCell>
                          <Link to={`/log/${encodeURIComponent(logUrl)}`}>
                            Log
                          </Link>
                        </TableCell>
                      </TableRow>
                    ),
                  )}
              </TableBody>
            </Table>
          </TableContainer>
        )}
        {mode === "card" && (
          <Grid container>
            {nodeList
              .slice(
                (page.pageNo - 1) * page.pageSize,
                page.pageNo * page.pageSize,
              )
              .map((e) => (
                <Grid item xs={6}>
                  <NodeCard node={e} />
                </Grid>
              ))}
          </Grid>
        )}
      </TitleCard>
    </div>
  );
};

export default Nodes;
