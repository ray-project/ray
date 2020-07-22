import { Typography } from "@material-ui/core";
import React from "react";
import { formatByteAmount } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import {
  ClusterFeature,
  NodeFeature,
  NodeFeatureData,
  NodeInfoFeature,
  WorkerFeature,
} from "./types";

export const ClusterSent: ClusterFeature = ({ nodes }) => {
  let totalSent = 0;
  for (const node of nodes) {
    totalSent += node.net[0];
  }
  return (
    <React.Fragment>{formatByteAmount(totalSent, "mebibyte")}/s</React.Fragment>
  );
};

export const NodeSent: NodeFeature = ({ node }) => (
  <React.Fragment>{formatByteAmount(node.net[0], "mebibyte")}/s</React.Fragment>
);

export const nodeSentAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.net[0];

export const WorkerSent: WorkerFeature = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);

const sentFeature: NodeInfoFeature = {
  id: "sent",
  ClusterFeature: ClusterSent,
  NodeFeature: NodeSent,
  WorkerFeature: WorkerSent,
  nodeAccessor: nodeSentAccessor,
};

export default sentFeature;
