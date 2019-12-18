import Link from "@material-ui/core/Link";
import Typography from "@material-ui/core/Typography";
import React from "react";
import { Link as RouterLink } from "react-router-dom";
import {
  ClusterFeatureComponent,
  NodeFeatureComponent,
  WorkerFeatureComponent
} from "./types";

export const makeClusterLogs = (logCounts: {
  [ip: string]: {
    perWorker: {
      [pid: string]: number;
    };
    total: number;
  };
}): ClusterFeatureComponent => ({ nodes }) => {
  let totalLogCount = 0;
  for (const node of nodes) {
    if (node.ip in logCounts) {
      totalLogCount += logCounts[node.ip].total;
    }
  }
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

export const makeNodeLogs = (logCounts: {
  perWorker: { [pid: string]: number };
  total: number;
}): NodeFeatureComponent => ({ node }) =>
  logCounts.total === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No logs
    </Typography>
  ) : (
    <Link component={RouterLink} to={`/logs/${node.hostname}`}>
      View all logs ({logCounts.total.toLocaleString()}{" "}
      {logCounts.total === 1 ? "line" : "lines"})
    </Link>
  );

export const makeWorkerLogs = (logCounts: {
  perWorker: { [pid: string]: number };
  total: number;
}): WorkerFeatureComponent => ({ node, worker }) =>
  logCounts.perWorker[worker.pid] === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No logs
    </Typography>
  ) : (
    <Link component={RouterLink} to={`/logs/${node.hostname}/${worker.pid}`}>
      View log ({logCounts.perWorker[worker.pid].toLocaleString()}{" "}
      {logCounts.perWorker[worker.pid] === 1 ? "line" : "lines"})
    </Link>
  );
