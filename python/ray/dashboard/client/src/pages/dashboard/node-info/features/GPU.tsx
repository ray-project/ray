import React from "react";
import UsageBar from "../../../../common/UsageBar";
import { getWeightedAverage, sum } from "../../../../common/util";
import {
  ClusterFeatureComponent,
  Node,
  NodeFeatureComponent,
  WorkerFeatureComponent,
} from "./types";

const clusterUtilization = (nodes: Array<Node>): number => {
  return getWeightedAverage(
    nodes.map((node) => ({
      weight: node.gpus.length,
      value: nodeUtilization(node),
    })),
  );
};

const nodeUtilization = (node: Node): number => {
  const utilizationSum = sum(node.gpus.map(gpu => gpu.utilization_gpu));
  const avgUtilization = utilizationSum / node.gpus.length;
  return avgUtilization;
};

export const ClusterGPU: ClusterFeatureComponent = ({ nodes }) => {
  const clusterAverageUtilization = clusterUtilization(nodes);
  return (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={clusterAverageUtilization}
        text={`${clusterAverageUtilization.toFixed(1)}%`}
      />
    </div>
  );
};

export const NodeGPU: NodeFeatureComponent = ({ node }) => {
  const nodeUtil = nodeUtilization(node);
  return (
    <div style={{ minWidth: 60 }}>
      <UsageBar percent={nodeUtil} text={`${nodeUtil.toFixed(1)}%`} />
    </div>
  );
};

export const WorkerGPU: WorkerFeatureComponent = ({ rayletWorker }) => {
  const workerRes = rayletWorker?.coreWorkerStats.usedResources;
  const workerUsedGPUResources = workerRes?.["GPU"] || "No";
  return (
    <div style={{ minWidth: 60 }}>
      <b>{workerUsedGPUResources} GPUs in use</b>
    </div>
  );
};
