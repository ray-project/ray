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
  plasmaStats,
}) => {
  const totalAvailable = sum(
    plasmaStats.map((s) => s.object_store_available_memory),
  );
  const totalUsed = sum(plasmaStats.map((s) => s.object_store_used_memory));
  return (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={100 * (totalUsed / totalAvailable)}
        text={formatUsage(totalUsed, totalAvailable, "mebibyte", false)}
      />
    </div>
  );
};

export const NodeObjectStoreMemory: NodeFeatureRenderFn = ({ plasmaStats }) => {
  if (!plasmaStats) {
    return (
      <Typography color="textSecondary" component="span" variant="inherit">
        N/A
      </Typography>
    );
  }
  const {
    object_store_used_memory,
    object_store_available_memory,
  } = plasmaStats;
  const usageRatio = object_store_used_memory / object_store_available_memory;
  return (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={usageRatio * 100}
        text={formatUsage(
          object_store_used_memory,
          object_store_available_memory,
          "mebibyte",
          false,
        )}
      />
    </div>
  );
};

export const nodeObjectStoreMemoryAccessor: Accessor<NodeFeatureData> = ({
  plasmaStats,
}) => {
  return plasmaStats?.object_store_used_memory ?? 0;
};

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
