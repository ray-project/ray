import { Typography } from "@material-ui/core";
import React from "react";
import { formatByteAmount } from "../../../../common/formatUtils";
import {
  ClusterFeatureComponent,
  NodeFeatureComponent,
  WorkerFeatureComponent,
} from "./types";

export const ClusterReceived: ClusterFeatureComponent = ({ nodes }) => {
  let totalReceived = 0;
  for (const node of nodes) {
    totalReceived += node.net[1];
  }
  return (
    <React.Fragment>
      {formatByteAmount(totalReceived, "mebibyte")}/s
    </React.Fragment>
  );
};

export const NodeReceived: NodeFeatureComponent = ({ node }) => (
  <React.Fragment>{formatByteAmount(node.net[1], "mebibyte")}/s</React.Fragment>
);

export const WorkerReceived: WorkerFeatureComponent = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);
