import Link from "@material-ui/core/Link";
import Typography from "@material-ui/core/Typography";
import React from "react";
import { Link as RouterLink } from "react-router-dom";
import { NodeFeatureComponent, WorkerFeatureComponent } from "./types";

export const makeNodeErrors = (errorCounts: {
  perWorker: { [pid: string]: number };
  total: number;
}): NodeFeatureComponent => ({ node }) =>
  errorCounts.total === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  ) : (
    <Link component={RouterLink} to={`/errors/${node.hostname}`}>
      View all errors ({errorCounts.total.toLocaleString()})
    </Link>
  );

export const makeWorkerErrors = (errorCounts: {
  perWorker: { [pid: string]: number };
  total: number;
}): WorkerFeatureComponent => ({ node, worker }) =>
  errorCounts.perWorker[worker.pid] === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      No errors
    </Typography>
  ) : (
    <Link component={RouterLink} to={`/errors/${node.hostname}/${worker.pid}`}>
      View errors ({errorCounts.perWorker[worker.pid].toLocaleString()})
    </Link>
  );
