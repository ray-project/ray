import { Typography } from "@material-ui/core";
import React from "react";
import SpanButton from "../../../../common/SpanButton";
import { Accessor } from "../../../../common/tableUtils";
import { sum } from "../../../../common/util";
import {
  ClusterFeatureRenderFn,
  NodeFeatureData,
  NodeFeatureRenderFn,
  NodeInfoFeature,
  WorkerFeatureData,
  WorkerFeatureRenderFn,
} from "./types";

const ClusterLogs: ClusterFeatureRenderFn = ({ nodes }) => {
  const totalLogCount = sum(nodes.map((n) => n.logCount ?? 0));
  return totalLogCount === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No logs
    </Typography>
  ) : (
    <React.Fragment>
      {totalLogCount.toLocaleString()} {totalLogCount === 1 ? "line" : "lines"}
    </React.Fragment>
  );
};

const makeNodeLogs = (
  setLogDialog: (nodeIp: string, pid: number | null) => void,
): NodeFeatureRenderFn => ({ node }) => {
  const logCount = node.logCount ?? 0;
  return logCount === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No logs
    </Typography>
  ) : (
    <SpanButton onClick={() => setLogDialog(node.ip, null)}>
      View all logs ({logCount.toLocaleString()}{" "}
      {node.logCount === 1 ? "line" : "lines"})
    </SpanButton>
  );
};

const nodeLogsAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.logCount ? sum(Object.values(node.logCount)) : 0;

const makeWorkerLogs = (
  setLogDialog: (nodeIp: string, pid: number | null) => void,
): WorkerFeatureRenderFn => ({ worker, node }) => {
  const logCount = worker.logCount ?? 0;
  return logCount !== 0 ? (
    <SpanButton onClick={() => setLogDialog(node.ip, worker.pid)}>
      View log ({logCount.toLocaleString()}{" "}
      {worker.logCount === 1 ? "line" : "lines"})
    </SpanButton>
  ) : (
    <Typography color="textSecondary" component="span" variant="inherit">
      No logs
    </Typography>
  );
};

const workerLogsAccessor: Accessor<WorkerFeatureData> = ({ worker }) =>
  worker.logCount ?? 0;

const makeLogsFeature = (
  setLogDialog: (nodeIp: string, pid: number | null) => void,
): NodeInfoFeature => ({
  id: "logs",
  ClusterFeatureRenderFn: ClusterLogs,
  WorkerFeatureRenderFn: makeWorkerLogs(setLogDialog),
  NodeFeatureRenderFn: makeNodeLogs(setLogDialog),
  workerAccessor: workerLogsAccessor,
  nodeAccessor: nodeLogsAccessor,
});

export default makeLogsFeature;
