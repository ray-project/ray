import { Typography } from "@material-ui/core";
import React from "react";
import SpanButton from "../../../../common/SpanButton";
import {
  ClusterFeatureComponent,
  NodeFeatureComponent,
  WorkerFeatureComponent,
} from "./types";

export const makeClusterErrors = (errorCounts: {
  [ip: string]: {
    perWorker: {
      [pid: string]: number;
    };
    total: number;
  };
}): ClusterFeatureComponent => ({ nodes }) => {
  let totalErrorCount = 0;
  for (const node of nodes) {
    if (node.ip in errorCounts) {
      totalErrorCount += errorCounts[node.ip].total;
    }
  }
  return totalErrorCount === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  ) : (
    <React.Fragment>
      {totalErrorCount.toLocaleString()}{" "}
      {totalErrorCount === 1 ? "error" : "errors"}
    </React.Fragment>
  );
};

export const makeNodeErrors = (
  errorCounts: {
    perWorker: { [pid: string]: number };
    total: number;
  },
  setErrorDialog: (hostname: string, pid: number | null) => void,
): NodeFeatureComponent => ({ node }) =>
  errorCounts.total === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  ) : (
    <SpanButton onClick={() => setErrorDialog(node.hostname, null)}>
      View all errors ({errorCounts.total.toLocaleString()})
    </SpanButton>
  );

export const makeWorkerErrors = (
  errorCounts: {
    perWorker: { [pid: string]: number };
    total: number;
  },
  setErrorDialog: (hostname: string, pid: number | null) => void,
): WorkerFeatureComponent => ({ node, worker }) =>
  errorCounts.perWorker[worker.pid] === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  ) : (
    <SpanButton onClick={() => setErrorDialog(node.hostname, worker.pid)}>
      View errors ({errorCounts.perWorker[worker.pid].toLocaleString()})
    </SpanButton>
  );
