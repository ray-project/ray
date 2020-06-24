import { Typography } from "@material-ui/core";
import React from "react";
import { formatByteAmount } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import {
  ClusterFeatureRenderFn,
  NodeFeatureRenderFn,
  NodeFeatureData,
  WorkerFeatureComponent,
} from "./types";

export const ClusterSent: ClusterFeatureRenderFn = ({ nodes }) => {
  let totalSent = 0;
  for (const node of nodes) {
    totalSent += node.net[0];
  }
  return (
    <React.Fragment>{formatByteAmount(totalSent, "mebibyte")}/s</React.Fragment>
  );
};

export const NodeSent: NodeFeatureRenderFn = ({ node }) => (
  <React.Fragment>{formatByteAmount(node.net[0], "mebibyte")}/s</React.Fragment>
);

export const NodeSentAccessor: Accessor<NodeFeatureData> = ({ node }) => (
  node.net[0]
);

export const WorkerSent: WorkerFeatureComponent = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);
