import { Typography } from "@material-ui/core";
import React from "react";
import { RayletWorkerStats } from "../../../../api";
import { Accessor } from "../../../../common/tableUtils";
import UsageBar from "../../../../common/UsageBar";
import { getWeightedAverage, sum } from "../../../../common/util";
import {
  ClusterFeatureRenderFn,
  Node,
  NodeFeatureData,
  NodeFeatureRenderFn,
  NodeInfoFeature,
  WorkerFeatureData,
  WorkerFeatureRenderFn,
} from "./types";

const clusterGPUUtilization = (nodes: Array<Node>): number => {
  const utils = nodes
    .map((node) => ({
      weight: node.gpus.length,
      value: nodeGPUUtilization(node),
    }))
    .filter((util) => !isNaN(util.value));
  if (utils.length === 0) {
    return NaN;
  }
  return getWeightedAverage(utils);
};

const nodeGPUUtilization = (node: Node): number => {
  if (!node.gpus || node.gpus.length === 0) {
    return NaN;
  }
  const utilizationSum = sum(node.gpus.map((gpu) => gpu.utilization_gpu));
  const avgUtilization = utilizationSum / node.gpus.length;
  return avgUtilization;
};

const nodeGPUAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  nodeGPUUtilization(node);

const ClusterGPU: ClusterFeatureRenderFn = ({ nodes }) => {
  const clusterAverageUtilization = clusterGPUUtilization(nodes);
  return (
    <div style={{ minWidth: 60 }}>
      {isNaN(clusterAverageUtilization) ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      ) : (
        <UsageBar
          percent={clusterAverageUtilization}
          text={`${clusterAverageUtilization.toFixed(1)}%`}
        />
      )}
    </div>
  );
};

const NodeGPU: NodeFeatureRenderFn = ({ node }) => {
  const nodeUtil = nodeGPUUtilization(node);
  return (
    <div style={{ minWidth: 60 }}>
      {isNaN(nodeUtil) ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      ) : (
        <UsageBar percent={nodeUtil} text={`${nodeUtil.toFixed(1)}%`} />
      )}
    </div>
  );
};

const WorkerGPU: WorkerFeatureRenderFn = ({ rayletWorker }) => {
  const aggregateAllocation = workerGPUUtilization(rayletWorker);
  let message;
  if (aggregateAllocation === undefined) {
    message = (
      <Typography color="textSecondary" component="span" variant="inherit">
        N/A
      </Typography>
    );
  } else {
    const plural = aggregateAllocation === 1 ? "" : "s";
    message = <b>{`${aggregateAllocation} GPU${plural} in use`}</b>;
  }
  return <div style={{ minWidth: 60 }}>{message}</div>;
};

const workerGPUUtilization = (rayletWorker: RayletWorkerStats | null) => {
  const workerRes = rayletWorker?.coreWorkerStats.usedResources;
  const workerUsedGPUResources = workerRes?.["GPU"];
  return (
    workerUsedGPUResources &&
    sum(
      workerUsedGPUResources.resourceSlots.map(
        (resourceSlot) => resourceSlot.allocation,
      ),
    )
  );
};

const workerGPUAccessor: Accessor<WorkerFeatureData> = ({ rayletWorker }) => {
  return workerGPUUtilization(rayletWorker) ?? 0;
};

const gpuFeature: NodeInfoFeature = {
  id: "gpu",
  ClusterFeatureRenderFn: ClusterGPU,
  NodeFeatureRenderFn: NodeGPU,
  WorkerFeatureRenderFn: WorkerGPU,
  nodeAccessor: nodeGPUAccessor,
  workerAccessor: workerGPUAccessor,
};

export default gpuFeature;
