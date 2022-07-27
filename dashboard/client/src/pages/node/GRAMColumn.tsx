import { Typography } from "@material-ui/core";
import React from "react";
import { NodeDetail } from "../../type/node";
import { Worker } from "../../type/worker";
import { GRAMEntry } from "../dashboard/node-info/features/GRAM";

const GRAM_COL_WIDTH = 120;

export const NodeGRAM = ({ node }: { node: NodeDetail }) => {
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

export const WorkerGRAM = ({
  worker,
  node,
}: {
  worker: Worker;
  node: NodeDetail;
}) => {
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
