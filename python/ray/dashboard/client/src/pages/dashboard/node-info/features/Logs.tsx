import { Typography } from "@material-ui/core";
import React from "react";
import SpanButton from "../../../../common/SpanButton";
import { Accessor } from "../../../../common/tableUtils";
import { sum } from "../../../../common/util";
import {
  ClusterFeature,
  NodeFeature,
  NodeFeatureData,
  NodeInfoFeature,
  WorkerFeature,
  WorkerFeatureData,
} from "./types";

const ClusterLogs: ClusterFeature = ({ nodes }) => {
  const totalLogCount = sum(nodes.map((node) => node.logCount));
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
): NodeFeature => ({ node }) =>
  node.logCount === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No logs
    </Typography>
  ) : (
    <SpanButton onClick={() => setLogDialog(node.hostname, null)}>
      View all logs ({node.logCount.toLocaleString()}{" "}
      {node.logCount === 1 ? "line" : "lines"})
    </SpanButton>
  );

const nodeLogsAccessor: Accessor<NodeFeatureData> = ({ node }) => node.logCount;

// TODO(mfitton) Make this work with new API
const makeWorkerLogs = (
  setLogDialog: (hostname: string, pid: number | null) => void,
): WorkerFeature => ({ node, worker }) =>
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
  ClusterFeature: ClusterLogs,
  WorkerFeature: makeWorkerLogs(setLogDialog),
  NodeFeature: makeNodeLogs(setLogDialog),
  workerAccessor: workerLogsAccessor,
  nodeAccessor: nodeLogsAccessor,
});

export default makeLogsFeature;
