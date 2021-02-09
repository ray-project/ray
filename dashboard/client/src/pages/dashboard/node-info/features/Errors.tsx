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

const ClusterErrors: ClusterFeatureRenderFn = ({ nodes }) => {
  const totalErrCount = sum(nodes.map((node) => node.errorCount ?? 0));
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
  setErrorDialog: (nodeIp: string, pid: number | null) => void,
): NodeFeatureRenderFn => ({ node }) => {
  const errorCount = node.errorCount ?? 0;
  return errorCount === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  ) : (
    <SpanButton onClick={() => setErrorDialog(node.ip, null)}>
      View all errors ({errorCount.toLocaleString()})
    </SpanButton>
  );
};

const nodeErrorsAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.errorCount ?? 0;

const makeWorkerErrors = (
  setErrorDialog: (nodeIp: string, pid: number | null) => void,
): WorkerFeatureRenderFn => ({ node, worker }) => {
  return worker.errorCount !== 0 ? (
    <SpanButton onClick={() => setErrorDialog(node.ip, worker.pid)}>
      View errors ({worker.errorCount?.toLocaleString()})
    </SpanButton>
  ) : (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  );
};

const workerErrorsAccessor: Accessor<WorkerFeatureData> = ({ worker }) =>
  worker.errorCount ?? 0;

const makeErrorsFeature = (
  setErrorDialog: (nodeIp: string, pid: number | null) => void,
): NodeInfoFeature => ({
  id: "errors",
  ClusterFeatureRenderFn: ClusterErrors,
  WorkerFeatureRenderFn: makeWorkerErrors(setErrorDialog),
  NodeFeatureRenderFn: makeNodeErrors(setErrorDialog),
  nodeAccessor: nodeErrorsAccessor,
  workerAccessor: workerErrorsAccessor,
});

export default makeErrorsFeature;
