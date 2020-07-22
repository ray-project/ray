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

export const ClusterReceived: ClusterFeature = ({ nodes }) => {
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

export const NodeReceived: NodeFeature = ({ node }) => (
  <React.Fragment>{formatByteAmount(node.net[1], "mebibyte")}/s</React.Fragment>
);

export const nodeReceivedAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.net[1];

export const WorkerReceived: WorkerFeature = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);

const receivedFeature: NodeInfoFeature = {
  id: "received",
  ClusterFeature: ClusterReceived,
  NodeFeature: NodeReceived,
  WorkerFeature: WorkerReceived,
  nodeAccessor: nodeReceivedAccessor,
};

export default receivedFeature;
