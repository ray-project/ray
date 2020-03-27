import { Typography } from "@material-ui/core";
import React from "react";
import { formatDuration } from "../../../../common/formatUtils";
import {
  ClusterFeatureComponent,
  NodeFeatureComponent,
  WorkerFeatureComponent,
} from "./types";

const getUptime = (bootTime: number) => Date.now() / 1000 - bootTime;

export const ClusterUptime: ClusterFeatureComponent = ({ nodes }) => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);

export const NodeUptime: NodeFeatureComponent = ({ node }) => (
  <React.Fragment>{formatDuration(getUptime(node.boot_time))}</React.Fragment>
);

export const WorkerUptime: WorkerFeatureComponent = ({ worker }) => (
  <React.Fragment>
    {formatDuration(getUptime(worker.create_time))}
  </React.Fragment>
);
