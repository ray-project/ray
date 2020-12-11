import { Typography } from "@material-ui/core";
import React from "react";
import { formatUsage } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import UsageBar from "../../../../common/UsageBar";
import { sum } from "../../../../common/util";
import {
  ClusterFeatureRenderFn,
  NodeFeatureData,
  NodeFeatureRenderFn,
  NodeInfoFeature,
  WorkerFeatureData,
  WorkerFeatureRenderFn,
} from "./types";

export const ClusterObjectStoreMemory: ClusterFeatureRenderFn = ({ nodes }) => {
  const totalAvailable = sum(
    nodes.map((n) => n.raylet.objectStoreAvailableMemory),
  );
  const totalUsed = sum(nodes.map((n) => n.raylet.objectStoreUsedMemory));
  const total = totalUsed + totalAvailable;
  return (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={100 * (totalUsed / total)}
        text={formatUsage(totalUsed, total, "mebibyte", false)}
      />
    </div>
  );
};

export const NodeObjectStoreMemory: NodeFeatureRenderFn = ({ node }) => {
  const totalAvailable = node.raylet.objectStoreAvailableMemory;
  const used = node.raylet.objectStoreUsedMemory;
  const total = totalAvailable + used;
  if (used === undefined || totalAvailable === undefined || total === 0) {
    return (
      <Typography color="textSecondary" component="span" variant="inherit">
        N/A
      </Typography>
    );
  }
  const usageRatio = used / total;
  return (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={usageRatio * 100}
        text={formatUsage(used, total, "mebibyte", false)}
      />
    </div>
  );
};

export const nodeObjectStoreMemoryAccessor: Accessor<NodeFeatureData> = ({
  node,
}) => node.raylet.objectStoreUsedMemory;

export const WorkerObjectStoreMemory: WorkerFeatureRenderFn = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);

export const workerObjectStoreMemoryAccessor: Accessor<WorkerFeatureData> = () =>
  0;

const objectStoreMemoryFeature: NodeInfoFeature = {
  id: "objectStoreMemory",
  ClusterFeatureRenderFn: ClusterObjectStoreMemory,
  NodeFeatureRenderFn: NodeObjectStoreMemory,
  WorkerFeatureRenderFn: WorkerObjectStoreMemory,
  nodeAccessor: nodeObjectStoreMemoryAccessor,
  workerAccessor: workerObjectStoreMemoryAccessor,
};

export default objectStoreMemoryFeature;
