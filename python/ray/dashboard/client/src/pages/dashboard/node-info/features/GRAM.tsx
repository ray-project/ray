import { Typography } from "@material-ui/core";
import React from "react";
import { GPUStats } from "../../../../api";
import { MiBRatio } from "../../../../common/formatUtils";
import UsageBar from "../../../../common/UsageBar";
import { getWeightedAverage, sum } from "../../../../common/util";
import {
  ClusterFeatureComponent,
  Node,
  NodeFeatureComponent,
  WorkerFeatureComponent,
} from "./types";

const nodeGRAMUtilization = (node: Node) => {
  const utilization = (gpu: GPUStats) => gpu.memory_used / gpu.memory_total;
  if (node.gpus.length === 0) {
    return NaN;
  }
  const utilizationSum = sum(node.gpus.map((gpu) => utilization(gpu)));
  const avgUtilization = utilizationSum / node.gpus.length;
  // Convert to a percent before returning
  return avgUtilization * 100;
};

const clusterGRAMUtilization = (nodes: Array<Node>) => {
  const utils = nodes
    .map((node) => ({
      weight: node.gpus.length,
      value: nodeGRAMUtilization(node),
    }))
    .filter((util) => !isNaN(util.value));
  if (utils.length === 0) {
    return NaN;
  }
  return getWeightedAverage(utils);
};

export const ClusterGRAM: ClusterFeatureComponent = ({ nodes }) => {
  const clusterAverageUtilization = clusterGRAMUtilization(nodes);
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

export const NodeGRAM: NodeFeatureComponent = ({ node }) => {
  const gramUtil = nodeGRAMUtilization(node);
  return (
    <div style={{ minWidth: 60 }}>
      {isNaN(gramUtil) ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      ) : (
        <UsageBar percent={gramUtil} text={`${gramUtil.toFixed(1)}%`} />
      )}
    </div>
  );
};

export const WorkerGRAM: WorkerFeatureComponent = ({ worker, node }) => {
  const workerProcessPerGPU = node.gpus
    .map((gpu) => gpu.processes)
    .map((processes) =>
      processes.find((process) => process.pid === worker.pid),
    );
  const workerUtilPerGPU = workerProcessPerGPU.map(
    (proc) => proc?.gpu_memory_usage || 0,
  );
  const totalNodeGRAM = sum(node.gpus.map((gpu) => gpu.memory_total));
  const usedGRAM = sum(workerUtilPerGPU);
  return (
    <div style={{ minWidth: 60 }}>
      {node.gpus.length === 0 ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      ) : (
        <UsageBar
          percent={100 * (usedGRAM / totalNodeGRAM)}
          text={MiBRatio(usedGRAM, totalNodeGRAM)}
        />
      )}
    </div>
  );
};
