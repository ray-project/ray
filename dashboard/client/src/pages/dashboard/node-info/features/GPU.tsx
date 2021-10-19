import { Box, Tooltip, Typography } from "@material-ui/core";
import React from "react";
import { GPUStats, ResourceSlot, Worker } from "../../../../api";
import { RightPaddedTypography } from "../../../../common/CustomTypography";
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

const GPU_COL_WIDTH = 120;

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
  if (node.gpus === null) {
    return NaN;
  }
  const gpusWithUtilInfo = node.gpus.filter((gpu) => gpu.utilizationGpu);
  if (gpusWithUtilInfo.length === 0) {
    return NaN;
  }

  const utilizationSum = sum(
    gpusWithUtilInfo.map((gpu) => gpu.utilizationGpu ?? 0),
  );
  const avgUtilization = utilizationSum / gpusWithUtilInfo.length;
  return avgUtilization;
};

const nodeGPUAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  nodeGPUUtilization(node);

const ClusterGPU: ClusterFeatureRenderFn = ({ nodes }) => {
  const clusterAverageUtilization = clusterGPUUtilization(nodes);
  return (
    <div style={{ minWidth: GPU_COL_WIDTH }}>
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
  const hasGPU = node.gpus !== undefined && node.gpus.length !== 0;
  return (
    <div style={{ minWidth: GPU_COL_WIDTH }}>
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
};

const NodeGPUEntry: React.FC<NodeGPUEntryProps> = ({ gpu, slot }) => {
  return (
    <Box display="flex" style={{ minWidth: GPU_COL_WIDTH }}>
      <Tooltip title={gpu.name}>
        <RightPaddedTypography variant="body1">[{slot}]:</RightPaddedTypography>
      </Tooltip>
      {gpu.utilizationGpu ? (
        <UsageBar
          percent={gpu.utilizationGpu}
          text={`${gpu.utilizationGpu.toFixed(1)}%`}
        />
      ) : (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      )}
    </Box>
  );
};

type WorkerGPUEntryProps = {
  resourceSlot: ResourceSlot;
};

const WorkerGPUEntry: React.FC<WorkerGPUEntryProps> = ({ resourceSlot }) => {
  const { allocation, slot } = resourceSlot;
  // This is a bit of  a dirty hack . For some reason, the slot GPU slot
  // 0 as assigned always shows up as undefined in the API response.
  // There are other times, such as a partial allocation, where we truly don't
  // know the slot, however this will just plug the hole of 0s coming through
  // as undefined. I have not been able to figure out the root cause.
  const slotMsg =
    allocation >= 1 && slot === undefined
      ? "0"
      : slot === undefined
      ? "?"
      : slot.toString();
  return (
    <Typography variant="body1">
      [{slotMsg}]: {allocation}
    </Typography>
  );
};

const WorkerGPU: WorkerFeatureRenderFn = ({ worker }) => {
  const workerRes = worker.coreWorkerStats[0]?.usedResources;
  const workerUsedGPUResources = workerRes?.["GPU"];
  let message;
  if (workerUsedGPUResources === undefined) {
    message = (
      <Typography color="textSecondary" component="span" variant="inherit">
        N/A
      </Typography>
    );
  } else {
    message = workerUsedGPUResources.resourceSlots
      .sort((slot1, slot2) => {
        if (slot1.slot === undefined && slot2.slot === undefined) {
          return 0;
        } else if (slot1.slot === undefined) {
          return 1;
        } else if (slot2.slot === undefined) {
          return -1;
        } else {
          return slot1.slot - slot2.slot;
        }
      })
      .map((resourceSlot) => <WorkerGPUEntry resourceSlot={resourceSlot} />);
  }
  return <div style={{ minWidth: 60 }}>{message}</div>;
};

const workerGPUUtilization = (worker: Worker | null) => {
  const workerRes = worker?.coreWorkerStats[0]?.usedResources;
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

const workerGPUAccessor: Accessor<WorkerFeatureData> = ({ worker }) => {
  return workerGPUUtilization(worker) ?? 0;
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
