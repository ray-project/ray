import { Typography } from "@material-ui/core";
import React from "react";
import SpanButton from "../../../../common/SpanButton";
import { Accessor } from "../../../../common/tableUtils";
import { sum } from "../../../../common/util";
import {
  ClusterFeature,
  Node,
  NodeFeatureData,
  NodeFeature,
  NodeInfoFeature,
  WorkerFeatureData,
  WorkerFeature,
} from "./types";

const nodeErrCount = (node: Node) =>
  node.error_count ? sum(Object.values(node.error_count)) : 0;

const ClusterErrors: ClusterFeature = ({ nodes }) => {
  const totalErrCount = sum(nodes.map(nodeErrCount));
  return totalErrCount === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  ) : (
    <React.Fragment>
      {totalErrCount.toLocaleString()}{" "}
      {totalErrCount === 1 ? "error" : "errors"}
    </React.Fragment>
  );
};

const makeNodeErrors = (
  setErrorDialog: (hostname: string, pid: number | null) => void,
): NodeFeature => ({ node }) => {
  const nodeErrorCount = nodeErrCount(node);
  return nodeErrorCount === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  ) : (
    <SpanButton onClick={() => setErrorDialog(node.hostname, null)}>
      View all errors ({nodeErrorCount.toLocaleString()})
    </SpanButton>
  );
};

const nodeErrorsAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  nodeErrCount(node);

const makeWorkerErrors = (
  setErrorDialog: (hostname: string, pid: number | null) => void,
): WorkerFeature => ({ node, worker }) => {
  const workerErrorCount = node.error_count?.[worker.pid] || 0;
  return workerErrorCount !== 0 ? (
    <SpanButton onClick={() => setErrorDialog(node.hostname, worker.pid)}>
      View errors ({workerErrorCount.toLocaleString()})
    </SpanButton>
  ) : (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  );
};

const workerErrorsAccessor: Accessor<WorkerFeatureData> = ({ node, worker }) =>
  node.error_count?.[worker.pid] || 0;

const makeErrorsFeature = (
  setErrorDialog: (hostname: string, pid: number | null) => void,
): NodeInfoFeature => ({
  id: "errors",
  ClusterFeature: ClusterErrors,
  WorkerFeature: makeWorkerErrors(setErrorDialog),
  NodeFeature: makeNodeErrors(setErrorDialog),
  nodeAccessor: nodeErrorsAccessor,
  workerAccessor: workerErrorsAccessor,
});

export default makeErrorsFeature;
