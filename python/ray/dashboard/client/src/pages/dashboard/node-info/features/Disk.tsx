import { Typography } from "@material-ui/core";
import React from "react";
import { formatUsage } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import UsageBar from "../../../../common/UsageBar";
import {
  ClusterFeatureComponent,
  NodeFeatureComponent,
  WorkerFeatureComponent,
  NodeFeatureData,
} from "./types";

export const ClusterDisk: ClusterFeatureComponent = ({ nodes }) => {
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

export const NodeDisk: NodeFeatureComponent = ({ node }) => (
  <UsageBar
    percent={(100 * node.disk["/"].used) / node.disk["/"].total}
    text={formatUsage(node.disk["/"].used, node.disk["/"].total, "gibibyte")}
  />
);
export const NodeDiskAccessor: Accessor<NodeFeatureData> = ({ node }) => (
  node.disk["/"].used
);

export const WorkerDisk: WorkerFeatureComponent = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);