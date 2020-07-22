import React from "react";
import { formatByteAmount, formatUsage } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import UsageBar from "../../../../common/UsageBar";
import {
  ClusterFeature,
  NodeFeature,
  NodeFeatureData,
  NodeInfoFeature,
  WorkerFeature,
  WorkerFeatureData,
} from "./types";

export const ClusterRAM: ClusterFeature = ({ nodes }) => {
  let used = 0;
  let total = 0;
  for (const node of nodes) {
    used += node.mem[0] - node.mem[1];
    total += node.mem[0];
  }
  return (
    <UsageBar
      percent={(100 * used) / total}
      text={formatUsage(used, total, "gibibyte")}
    />
  );
};

export const NodeRAM: NodeFeature = ({ node }) => (
  <UsageBar
    percent={(100 * (node.mem[0] - node.mem[1])) / node.mem[0]}
    text={formatUsage(node.mem[0] - node.mem[1], node.mem[0], "gibibyte")}
  />
);

export const nodeRAMAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  100 * (node.mem[0] - node.mem[1]);

export const WorkerRAM: WorkerFeature = ({ node, worker }) => (
  <UsageBar
    percent={(100 * worker.memoryInfo.rss) / node.mem[0]}
    text={formatByteAmount(worker.memoryInfo.rss, "mebibyte")}
  />
);

export const workerRAMAccessor: Accessor<WorkerFeatureData> = ({ worker }) =>
  worker.memoryInfo.rss;

const ramFeature: NodeInfoFeature = {
  id: "ram",
  ClusterFeature: ClusterRAM,
  NodeFeature: NodeRAM,
  WorkerFeature: WorkerRAM,
  nodeAccessor: nodeRAMAccessor,
  workerAccessor: workerRAMAccessor,
};

export default ramFeature;
