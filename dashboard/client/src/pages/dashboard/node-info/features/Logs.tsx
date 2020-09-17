import { Typography } from "@material-ui/core";
import React from "react";
import SpanButton from "../../../../common/SpanButton";
import { Accessor } from "../../../../common/tableUtils";
import { sum } from "../../../../common/util";
import {
  ClusterFeatureRenderFn,
  Node,
  NodeFeatureData,
  NodeFeatureRenderFn,
  NodeInfoFeature,
  WorkerFeatureData,
  WorkerFeatureRenderFn,
} from "./types";

const nodeLogCount = (node: Node) =>
  node.logCount ? sum(Object.values(node.logCount)) : 0;

const ClusterLogs: ClusterFeatureRenderFn = ({ nodes }) => {
  const totalLogCount = sum(nodes.map(nodeLogCount));
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
  setLogDialog: (hostname: string, pid: number | null) => void,
): NodeFeatureRenderFn => ({ node }) => {
  const logCount = nodeLogCount(node);
  return logCount === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No logs
    </Typography>
  ) : (
    <SpanButton onClick={() => setLogDialog(node.hostname, null)}>
      View all logs ({logCount.toLocaleString()}{" "}
      {logCount === 1 ? "line" : "lines"})
    </SpanButton>
  );
};

const nodeLogsAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.logCount ? sum(Object.values(node.logCount)) : 0;

const makeWorkerLogs = (
  setLogDialog: (hostname: string, pid: number | null) => void,
): WorkerFeatureRenderFn => ({ worker, node }) =>
  worker.logCount !== 0 ? (
    <SpanButton onClick={() => setLogDialog(node.hostname, worker.pid)}>
      View log ({worker.logCount.toLocaleString()}{" "}
      {worker.logCount === 1 ? "line" : "lines"})
    </SpanButton>
  ) : (
    <Typography color="textSecondary" component="span" variant="inherit">
      No logs
    </Typography>
  );

const workerLogsAccessor: Accessor<WorkerFeatureData> = ({ worker }) =>
  worker.logCount;

const makeLogsFeature = (
  setLogDialog: (hostname: string, pid: number | null) => void,
): NodeInfoFeature => ({
  id: "logs",
  ClusterFeatureRenderFn: ClusterLogs,
  WorkerFeatureRenderFn: makeWorkerLogs(setLogDialog),
  NodeFeatureRenderFn: makeNodeLogs(setLogDialog),
  workerAccessor: workerLogsAccessor,
  nodeAccessor: nodeLogsAccessor,
});

export default makeLogsFeature;
