import { Box, Tooltip, Typography } from "@material-ui/core";
import React from "react";
import { GPUStats } from "../../../../api";
import { RightPaddedTypography } from "../../../../common/CustomTypography";
import { MiBRatioNoPercent } from "../../../../common/formatUtils";
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

const GRAM_COL_WIDTH = 120;

const nodeGRAMUtilization = (node: Node) => {
  const utilization = (gpu: GPUStats) => {
    if (!gpu.memoryUsed || !gpu.memoryTotal) {
      return NaN;
    }
    return gpu.memoryUsed / gpu.memoryTotal;
  };
  const gramUtils = node.gpus.map(utilization).filter((util) => !!util);
  if (gramUtils.length === 0) {
    return NaN;
  }
  const utilizationSum = sum(node.gpus.map((gpu) => utilization(gpu)));
  const avgUtilization = utilizationSum / node.gpus.length;
  // Convert to a percent before returning
  return avgUtilization * 100;
};

const nodeGRAMAccessor: Accessor<NodeFeatureData> = ({ node }) => {
  const nodeGRAMUtil = nodeGRAMUtilization(node);
  return isNaN(nodeGRAMUtil) ? -1 : nodeGRAMUtil;
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

export const ClusterGRAM: ClusterFeatureRenderFn = ({ nodes }) => {
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

export const NodeGRAM: NodeFeatureRenderFn = ({ node }) => {
  const nodeGRAMEntries = node.gpus.map((gpu, i) => {
    const props = {
      gpuName: gpu.name,
      utilization: gpu.memoryUsed,
      total: gpu.memoryTotal,
      slot: i,
    };
    return <GRAMEntry {...props} />;
  });
  return (
    <div style={{ minWidth: 60 }}>
      {nodeGRAMEntries.length === 0 ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      ) : (
        <div style={{ minWidth: GRAM_COL_WIDTH }}>{nodeGRAMEntries}</div>
      )}
    </div>
  );
};

type GRAMEntryProps = {
  gpuName: string;
  slot: number;
  utilization: number;
  total: number;
};

const GRAMEntry: React.FC<GRAMEntryProps> = ({
  gpuName,
  slot,
  utilization,
  total,
}) => {
  const ratioStr = MiBRatioNoPercent(utilization, total);
  return (
    <Box display="flex" style={{ minWidth: GRAM_COL_WIDTH }}>
      <Tooltip title={gpuName}>
        <RightPaddedTypography variant="body1">
          [{slot}]: {ratioStr}
        </RightPaddedTypography>
      </Tooltip>
    </Box>
  );
};

export const WorkerGRAM: WorkerFeatureRenderFn = ({ worker, node }) => {
  const workerGRAMEntries = node.gpus
    .map((gpu, i) => {
      const process = gpu.processes.find(
        (process) => process.pid === worker.pid,
      );
      if (!process) {
        return undefined;
      }
      const props = {
        gpuName: gpu.name,
        total: gpu.memoryTotal,
        utilization: process.gpuMemoryUsage,
        slot: i,
      };
      return <GRAMEntry {...props} />;
    })
    .filter((entry) => entry !== undefined);

  return workerGRAMEntries.length === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      N/A
    </Typography>
  ) : (
    <div style={{ minWidth: GRAM_COL_WIDTH }}>{workerGRAMEntries}</div>
  );
};

const workerGRAMUtilization = (worker: any, node: Node) => {
  const workerProcessPerGPU = node.gpus
    .map((gpu) => gpu.processes)
    .map((processes) =>
      processes.find((process) => process.pid === worker.pid),
    );
  const workerUtilPerGPU = workerProcessPerGPU.map(
    (proc) => proc?.gpuMemoryUsage || 0,
  );
  return sum(workerUtilPerGPU);
};

const workerGRAMAccessor: Accessor<WorkerFeatureData> = ({ worker, node }) => {
  if (node.gpus.length === 0) {
    return -1;
  }
  return workerGRAMUtilization(worker, node);
};

const gramFeature: NodeInfoFeature = {
  id: "gram",
  ClusterFeatureRenderFn: ClusterGRAM,
  NodeFeatureRenderFn: NodeGRAM,
  WorkerFeatureRenderFn: WorkerGRAM,
  nodeAccessor: nodeGRAMAccessor,
  workerAccessor: workerGRAMAccessor,
};

export default gramFeature;
