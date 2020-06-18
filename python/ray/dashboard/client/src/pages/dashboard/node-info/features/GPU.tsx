import { Typography, Tooltip } from "@material-ui/core";
import React from "react";
import UsageBar from "../../../../common/UsageBar";
import { getWeightedAverage, sum } from "../../../../common/util";
import { ResourceSlot, GPUStats } from "../../../../api";
import {
  ClusterFeatureComponent,
  Node,
  NodeFeatureComponent,
  WorkerFeatureComponent,
} from "./types";

const clusterUtilization = (nodes: Array<Node>): number => {
  const utils = nodes
    .map((node) => ({ weight: node.gpus.length, value: nodeAverageUtilization(node) }))
    .filter((util) => !isNaN(util.value));
  if (utils.length === 0) {
    return NaN;
  }
  return getWeightedAverage(utils);
};

const nodeAverageUtilization = (node: Node): number => {
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

export const NodeGPU: NodeFeatureComponent = ({ node }) => {
  const hasGPU = (node.gpus !== undefined) && (node.gpus.length !== 0)
  return (
    <div style={{ minWidth: 60 }}>
      {hasGPU ? (
        node.gpus.map((gpu, i) => <NodeGPUEntry gpu={gpu} slot={i} />)
      ) : (
          <Typography color="textSecondary" component="span" variant="inherit">
            N/A
          </Typography>
        )}
    </div>
  );
};

type NodeGPUEntryProps = {
  slot: number;
  gpu: GPUStats;
}
const NodeGPUEntry: React.FC<NodeGPUEntryProps> = ({ slot, gpu }) => {
  const utilPercent = gpu.utilization_gpu * 100;
  return (
    <div>
      <Tooltip title={gpu.name}>
        <Typography>[{slot}]</Typography>
      </Tooltip>
      <UsageBar percent={utilPercent} text={`${utilPercent.toFixed(1)}`} />
    </div>
  )
}

export const WorkerGPU: WorkerFeatureComponent = ({ rayletWorker }) => {
  const workerRes = rayletWorker?.coreWorkerStats.usedResources;
  const workerUsedGPUResources = workerRes?.["GPU"];
  let message;
  if (workerUsedGPUResources === undefined) {
    message = (
      <Typography color="textSecondary" component="span" variant="inherit">
        N/A
      </Typography>
    );
  } else {
    const aggregateAllocation = sum(
      workerUsedGPUResources.resourceSlots.map(
        (resourceSlot) => resourceSlot.allocation,
      ),
    );
    const plural = aggregateAllocation === 1 ? "" : "s";
    message = <b>{`${aggregateAllocation} GPU${plural} in use`}</b>;
  }
  return <div style={{ minWidth: 60 }}>{message}</div>;
};
