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

export const ClusterObjectStoreMemory: ClusterFeatureRenderFn = ({
  nodes
}) => {
  const totalAvailable = sum(
    nodes.map(n => n.plasma.availableMemory),
  );

  const totalUsed = sum(nodes.map(n => n.plasma.usedMemory));
  return (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={100 * (totalUsed / totalAvailable)}
        text={formatUsage(totalUsed, totalAvailable, "mebibyte", false)}
      />
    </div>
  );
};

export const NodeObjectStoreMemory: NodeFeatureRenderFn = ({ node }) => {
  if (!node.plasma) {
    return (
      <Typography color="textSecondary" component="span" variant="inherit">
        N/A
      </Typography>
    );
  }
  const {
    usedMemory,
    availableMemory,
  } = node.plasma;
  const usageRatio = usedMemory / availableMemory;
  return (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={usageRatio * 100}
        text={formatUsage(
          usedMemory,
          availableMemory,
          "mebibyte",
          false,
        )}
      />
    </div>
  );
};

export const nodeObjectStoreMemoryAccessor: Accessor<NodeFeatureData> = ({
  node
}) => node.plasma?.usedMemory ?? 0;


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
