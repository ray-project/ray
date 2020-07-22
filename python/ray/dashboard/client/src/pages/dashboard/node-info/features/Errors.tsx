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

const ClusterErrors: ClusterFeature = ({ nodes }) => {
  const totalErrCount = sum(nodes.map((node) => node.errorCounts));
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
): NodeFeature => ({ node }) =>
  node.errorCounts === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  ) : (
    <SpanButton onClick={() => setErrorDialog(node.hostname, null)}>
      View all errors ({node.errorCounts.toLocaleString()})
    </SpanButton>
  );

const nodeErrorsAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.errorCounts;

const makeWorkerErrors = (
  setErrorDialog: (hostname: string, pid: number | null) => void,
): WorkerFeature => ({ node, worker }) => {
  // Todo, support this calculation in the new API.
  return (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  );
};

const workerErrorsAccessor: Accessor<WorkerFeatureData> = ({ node, worker }) =>
  0;

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
