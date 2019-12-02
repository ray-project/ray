import Link from "@material-ui/core/Link";
import Typography from "@material-ui/core/Typography";
import React from "react";
import { Link as RouterLink } from "react-router-dom";
import { makeFeature } from "./Feature";

export const makeLogsFeature = (logCounts: {
  perWorker: { [pid: string]: number };
  total: number;
}) =>
  makeFeature({
    getFeatureForNode: ({ node }) =>
      logCounts.total === 0 ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          No logs
        </Typography>
      ) : (
        <Link component={RouterLink} to={`/logs/${node.hostname}`}>
          View all logs ({logCounts.total.toLocaleString()}{" "}
          {logCounts.total === 1 ? "line" : "lines"})
        </Link>
      ),
    getFeatureForWorker: ({ node, worker }) =>
      logCounts.perWorker[worker.pid] === 0 ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          No logs
        </Typography>
      ) : (
        <Link
          component={RouterLink}
          to={`/logs/${node.hostname}/${worker.pid}`}
        >
          View log ({logCounts.perWorker[worker.pid].toLocaleString()}{" "}
          {logCounts.perWorker[worker.pid] === 1 ? "line" : "lines"})
        </Link>
      )
  });
