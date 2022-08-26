import { Typography } from "@material-ui/core";
import React from "react";
import { NodeDetail } from "../../type/node";
import { Worker } from "../../type/worker";
import {
  GPU_COL_WIDTH,
  NodeGPUEntry,
  WorkerGPUEntry,
} from "../dashboard/node-info/features/GPU";

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
