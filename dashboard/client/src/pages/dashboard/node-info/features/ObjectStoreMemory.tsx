import { Typography } from "@material-ui/core";
import React from "react";
import { formatUsage } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import UsageBar from "../../../../common/UsageBar";
import { sum } from "../../../../common/util";
import { ViewData } from '../../../../api';
import {
  ClusterFeatureRenderFn,
  NodeFeatureData,
  NodeFeatureRenderFn,
  NodeInfoFeature,
  WorkerFeatureData,
  WorkerFeatureRenderFn,
} from "./types";

const getDouble = (view: ViewData | undefined) =>
  view?.measures[0]?.doubleValue ?? 0;

export const ClusterObjectStoreMemory: ClusterFeatureRenderFn = ({
  nodes
}) => {
  const totalAvailable = sum(
    nodes.map(n => getDouble(n.raylet.viewData?.objectStoreAvailableMemory)),
  );
  const totalUsed = sum(nodes.map(n => getDouble(n.raylet.viewData.objectStoreUsedMemory)));
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
  const total = getDouble(node.raylet.viewData?.objectStoreAvailableMemory); 
  const used = getDouble(node.raylet.viewData?.objectStoreUsedMemory);
  if (!used || !total) {
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
        text={formatUsage(
          used,
          total,
          "mebibyte",
          false,
        )}
      />
    </div>
  );
};

export const nodeObjectStoreMemoryAccessor: Accessor<NodeFeatureData> = ({
  node
}) => getDouble(node.raylet.viewData?.objectStoreUsedMemory);

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
