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

export const ClusterReceived: ClusterFeatureRenderFn = ({ nodes }) => {
  let totalReceived = 0;
  for (const node of nodes) {
    totalReceived += node.networkSpeed[1];
  }
  return (
    <React.Fragment>
      {formatByteAmount(totalReceived, "mebibyte")}/s
    </React.Fragment>
  );
};

export const NodeReceived: NodeFeatureRenderFn = ({ node }) => (
  <React.Fragment>
    {formatByteAmount(node.networkSpeed[1], "mebibyte")}/s
  </React.Fragment>
);

export const nodeReceivedAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.networkSpeed[1];

export const WorkerReceived: WorkerFeatureRenderFn = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);

const receivedFeature: NodeInfoFeature = {
  id: "received",
  ClusterFeatureRenderFn: ClusterReceived,
  NodeFeatureRenderFn: NodeReceived,
  WorkerFeatureRenderFn: WorkerReceived,
  nodeAccessor: nodeReceivedAccessor,
};

export default receivedFeature;
