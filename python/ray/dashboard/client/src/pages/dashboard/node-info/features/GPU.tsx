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
  const utils = nodes
    .map((node) => ({ weight: node.gpus.length, value: nodeUtilization(node) }))
    .filter((util) => !isNaN(util.value));
  if (utils.length === 0) {
    return NaN;
  }
  return getWeightedAverage(utils);
};

const nodeUtilization = (node: Node): number => {
  if (!node.gpus || node.gpus.length === 0) {
    return NaN;
  }
  const utilizationSum = sum(node.gpus.map((gpu) => gpu.utilization_gpu));
  const avgUtilization = utilizationSum / node.gpus.length;
  return avgUtilization;
};

export const ClusterGPU: ClusterFeatureComponent = ({ nodes }) => {
  const clusterAverageUtilization = clusterUtilization(nodes);
  return (
    <div style={{ minWidth: 60 }}>
      {isNaN(clusterAverageUtilization) ? (
        <b>No GPUs</b>
      ) : (
        <UsageBar
          percent={clusterAverageUtilization}
          text={`${clusterAverageUtilization.toFixed(1)}%`}
        />
      )}
    </div>
  );
};

export const NodeGPU: NodeFeatureComponent = ({ node }) => {
  const nodeUtil = nodeUtilization(node);
  return (
    <div style={{ minWidth: 60 }}>
      {isNaN(nodeUtil) ? (
        <b>No GPUs</b>
      ) : (
        <UsageBar percent={nodeUtil} text={`${nodeUtil.toFixed(1)}%`} />
      )}
    </div>
  );
};

export const WorkerGPU: WorkerFeatureComponent = ({ rayletWorker }) => {
  const workerRes = rayletWorker?.coreWorkerStats.usedResources;
  const workerUsedGPUResources = workerRes?.["GPU"] || NaN;
  const message = isNaN(workerUsedGPUResources)
    ? "No GPUs"
    : `${workerUsedGPUResources} GPUs in use`;
  return (
    <div style={{ minWidth: 60 }}>
      <b>{message}</b>
    </div>
  );
};
