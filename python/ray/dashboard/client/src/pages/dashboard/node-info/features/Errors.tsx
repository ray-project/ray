import { Typography } from "@material-ui/core";
import React from "react";
import SpanButton from "../../../../common/SpanButton";
import { sum } from "../../../../common/util";
import {
  ClusterFeatureRenderFn,
  Node,
  NodeFeatureRenderFn,
  WorkerFeatureRenderFn,
} from "./types";

const nodeErrCount = (node: Node) =>
  node.error_count ? sum(Object.values(node.error_count)) : 0;

const ClusterErrors: ClusterFeatureRenderFn = ({ nodes }) => {
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
): NodeFeatureRenderFn => ({ node }) => {
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

const makeWorkerErrors = (
  setErrorDialog: (hostname: string, pid: number | null) => void,
): WorkerFeatureRenderFn => ({ node, worker }) => {
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

const makeErrorsFeature = (
  setErrorDialog: (hostname: string, pid: number | null) => void,
) => ({
  ClusterFeatureRenderFn: ClusterErrors,
  WorkerFeatureRenderFn: makeWorkerErrors(setErrorDialog),
  NodeFeatureRenderFn: makeNodeErrors(setErrorDialog),
});

export default makeErrorsFeature;
