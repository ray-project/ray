import { Typography } from "@material-ui/core";
import React from "react";
import { formatUsage } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import UsageBar from "../../../../common/UsageBar";
import {
  ClusterFeatureRenderFn,
  NodeFeatureData,
  NodeFeatureRenderFn,
  NodeInfoFeature,
  WorkerFeatureRenderFn,
} from "./types";

export const ClusterDisk: ClusterFeatureRenderFn = ({ nodes }) => {
  let used = 0;
  let total = 0;
  for (const node of nodes) {
    if ("/" in node.disk) {
      used += node.disk["/"].used;
      total += node.disk["/"].total;
    }
  }
  return (
    <UsageBar
      percent={(100 * used) / total}
      text={formatUsage(used, total, "gibibyte", true)}
    />
  );
};

export const NodeDisk: NodeFeatureRenderFn = ({ node }) => (
  <UsageBar
    percent={(100 * node.disk["/"].used) / node.disk["/"].total}
    text={formatUsage(
      node.disk["/"].used,
      node.disk["/"].total,
      "gibibyte",
      true,
    )}
  />
);

export const nodeDiskAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.disk["/"].used;

export const WorkerDisk: WorkerFeatureRenderFn = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);

const diskFeature: NodeInfoFeature = {
  id: "disk",
  ClusterFeatureRenderFn: ClusterDisk,
  NodeFeatureRenderFn: NodeDisk,
  WorkerFeatureRenderFn: WorkerDisk,
  nodeAccessor: nodeDiskAccessor,
};

export default diskFeature;
