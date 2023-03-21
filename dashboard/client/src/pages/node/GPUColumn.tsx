import { Box, Tooltip, Typography } from "@material-ui/core";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import UsageBar from "../../common/UsageBar";
import { GPUStats, NodeDetail } from "../../type/node";
import { Worker } from "../../type/worker";

export const GPU_COL_WIDTH = 120;

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
          <NodeGPUEntry key={gpu.uuid} gpu={gpu} slot={gpu.index} />
        ))
      ) : (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      )}
    </div>
  );
};

export const WorkerGpuRow = ({
  worker,
  node,
}: {
  worker: Worker;
  node: NodeDetail;
}) => {
  const workerGPUEntries = (node.gpus ?? [])
    .map((gpu, i) => {
      const process = gpu.processes?.find(
        (process) => process.pid === worker.pid,
      );
      if (!process) {
        return undefined;
      }
      return <NodeGPUEntry key={gpu.uuid} gpu={gpu} slot={gpu.index} />;
    })
    .filter((entry) => entry !== undefined);

  return workerGPUEntries.length === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      N/A
    </Typography>
  ) : (
    <div style={{ minWidth: GPU_COL_WIDTH }}>{workerGPUEntries}</div>
  );
};
