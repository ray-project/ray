import { Typography } from "@material-ui/core";
import React from "react";
import { formatUsage } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import UsageBar from "../../../../common/UsageBar";
import {
  ClusterFeature,
  NodeFeature,
  NodeFeatureData,
  NodeInfoFeature,
  WorkerFeature,
} from "./types";

export const ClusterDisk: ClusterFeature = ({ nodes }) => {
  let used = 0;
  let total = 0;
  for (const node of nodes) {
    used += node.disk["/"].used;
    total += node.disk["/"].total;
  }
  return (
    <UsageBar
      percent={(100 * used) / total}
      text={formatUsage(used, total, "gibibyte")}
    />
  );
};

export const NodeDisk: NodeFeature = ({ node }) => (
  <UsageBar
    percent={(100 * node.disk["/"].used) / node.disk["/"].total}
    text={formatUsage(node.disk["/"].used, node.disk["/"].total, "gibibyte")}
  />
);

export const nodeDiskAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.disk["/"].used;

export const WorkerDisk: WorkerFeature = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);

const diskFeature: NodeInfoFeature = {
  id: "disk",
  ClusterFeature: ClusterDisk,
  NodeFeature: NodeDisk,
  WorkerFeature: WorkerDisk,
  nodeAccessor: nodeDiskAccessor,
};

export default diskFeature;
