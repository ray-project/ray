import { Box, Tooltip, Typography } from "@material-ui/core";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import UsageBar from "../../common/UsageBar";
import { GPUStats, NodeDetail } from "../../type/node";
import { ResourceSlot, Worker } from "../../type/worker";

export const GPU_COL_WIDTH = 120;

type WorkerGPUEntryProps = {
  resourceSlot: ResourceSlot;
};

export const WorkerGPUEntry: React.FC<WorkerGPUEntryProps> = ({
  resourceSlot,
}) => {
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

export type NodeGPUEntryProps = {
  slot: number;
  gpu: GPUStats;
};

export const NodeGPUEntry: React.FC<NodeGPUEntryProps> = ({ gpu, slot }) => {
  return (
    <Box display="flex" style={{ minWidth: GPU_COL_WIDTH }}>
      <Tooltip title={gpu.name}>
        <RightPaddedTypography variant="body1">[{slot}]:</RightPaddedTypography>
      </Tooltip>
      {gpu.utilizationGpu !== undefined ? (
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

export const NodeGPUView = ({ node }: { node: NodeDetail }) => {
  return (
    <div style={{ minWidth: GPU_COL_WIDTH }}>
      {node.gpus !== undefined && node.gpus.length !== 0 ? (
        node.gpus.map((gpu, i) => (
          <NodeGPUEntry key={gpu.uuid} gpu={gpu} slot={i} />
        ))
      ) : (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      )}
    </div>
  );
};

export const WorkerGPU = ({ worker }: { worker: Worker }) => {
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
      .map((resourceSlot) => (
        <WorkerGPUEntry key={resourceSlot.slot} resourceSlot={resourceSlot} />
      ));
  }
  return <div style={{ minWidth: 60 }}>{message}</div>;
};
