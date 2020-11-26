import React from "react";
import { formatByteAmount, formatUsage } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import UsageBar from "../../../../common/UsageBar";
import {
  ClusterFeatureRenderFn,
  NodeFeatureData,
  NodeFeatureRenderFn,
  NodeInfoFeature,
  WorkerFeatureData,
  WorkerFeatureRenderFn,
} from "./types";

export const ClusterRAM: ClusterFeatureRenderFn = ({ nodes }) => {
  let used = 0;
  let total = 0;
  for (const node of nodes) {
    used += node.mem[0] - node.mem[1];
    total += node.mem[0];
  }
  return (
    <UsageBar
      percent={(100 * used) / total}
      text={formatUsage(used, total, "gibibyte", true)}
    />
  );
};

export const NodeRAM: NodeFeatureRenderFn = ({ node }) => (
  <UsageBar
    percent={(100 * (node.mem[0] - node.mem[1])) / node.mem[0]}
    text={formatUsage(node.mem[0] - node.mem[1], node.mem[0], "gibibyte", true)}
  />
);

export const nodeRAMAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  100 * (node.mem[0] - node.mem[1]);

export const WorkerRAM: WorkerFeatureRenderFn = ({ node, worker }) => (
  <UsageBar
    percent={(100 * worker.memoryInfo.rss) / node.mem[0]}
    text={formatByteAmount(worker.memoryInfo.rss, "mebibyte")}
  />
);

export const workerRAMAccessor: Accessor<WorkerFeatureData> = ({ worker }) =>
  worker.memoryInfo.rss;

const ramFeature: NodeInfoFeature = {
  id: "ram",
  ClusterFeatureRenderFn: ClusterRAM,
  NodeFeatureRenderFn: NodeRAM,
  WorkerFeatureRenderFn: WorkerRAM,
  nodeAccessor: nodeRAMAccessor,
  workerAccessor: workerRAMAccessor,
};

export default ramFeature;
