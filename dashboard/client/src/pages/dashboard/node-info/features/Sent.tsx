import { Typography } from "@material-ui/core";
import React from "react";
import { formatByteAmount } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import {
  ClusterFeatureRenderFn,
  NodeFeatureData,
  NodeFeatureRenderFn,
  NodeInfoFeature,
  WorkerFeatureRenderFn,
} from "./types";

export const ClusterSent: ClusterFeatureRenderFn = ({ nodes }) => {
  let totalSent = 0;
  for (const node of nodes) {
    totalSent += node.networkSpeed[0];
  }
  return (
    <React.Fragment>{formatByteAmount(totalSent, "mebibyte")}/s</React.Fragment>
  );
};

export const NodeSent: NodeFeatureRenderFn = ({ node }) => (
  <React.Fragment>
    {formatByteAmount(node.networkSpeed[0], "mebibyte")}/s
  </React.Fragment>
);

export const nodeSentAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.networkSpeed[0];

export const WorkerSent: WorkerFeatureRenderFn = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);

const sentFeature: NodeInfoFeature = {
  id: "sent",
  ClusterFeatureRenderFn: ClusterSent,
  NodeFeatureRenderFn: NodeSent,
  WorkerFeatureRenderFn: WorkerSent,
  nodeAccessor: nodeSentAccessor,
};

export default sentFeature;
