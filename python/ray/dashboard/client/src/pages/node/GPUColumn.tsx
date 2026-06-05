import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import UsageBar from "../../common/UsageBar";
import { GPUStats, NodeDetail, TPUStats } from "../../type/node";
import {
  normalizeAccelerators,
  UnifiedAcceleratorStat,
} from "../../util/accelerator";
import { memoryConverter } from "../../util/converter";

export type NodeGPUEntryProps = {
  slot: number;
  accelerator: UnifiedAcceleratorStat;
};

const GpuTooltip = ({ gpu }: { gpu: GPUStats }) => {
  return (
    <Box>
      <Typography variant="body2">Name: {gpu.name}</Typography>
      {gpu.temperatureC !== undefined && (
        <Typography variant="body2">
          Temperature: {gpu.temperatureC}°C
        </Typography>
      )}
      {gpu.powerMw !== undefined && (
        <Typography variant="body2">
          Power Draw: {(gpu.powerMw / 1000).toFixed(1)} W
        </Typography>
      )}
    </Box>
  );
};

const TpuTooltip = ({ tpu }: { tpu: TPUStats }) => {
  const tensorcoreUtilization = tpu.tensorcoreUtilization ?? 0;
  const hbmUtilization = tpu.hbmUtilization ?? 0;

  return (
    <Box>
      <Typography variant="body2">Name: {tpu.name}</Typography>
      <Typography variant="body2">Type: {tpu.tpuType}</Typography>
      <Typography variant="body2">Topology: {tpu.tpuTopology}</Typography>
      <Typography variant="body2">
        Tensorcore: {tensorcoreUtilization.toFixed(1)}%
      </Typography>
      <Typography variant="body2">
        HBM Bandwidth: {hbmUtilization.toFixed(1)}%
      </Typography>
      <Typography variant="body2">
        Used Memory: {memoryConverter(tpu.memoryUsed)} /{" "}
        {memoryConverter(tpu.memoryTotal)}
      </Typography>
      <Typography variant="body2">
        Free Memory: {memoryConverter(tpu.memoryTotal - tpu.memoryUsed)}
      </Typography>
    </Box>
  );
};

export const NodeGPUEntry: React.FC<NodeGPUEntryProps> = ({
  accelerator,
  slot,
}) => {
  const title =
    accelerator.type === "GPU" && accelerator.rawGpu ? (
      <GpuTooltip gpu={accelerator.rawGpu} />
    ) : accelerator.type === "TPU" && accelerator.rawTpu ? (
      <TpuTooltip tpu={accelerator.rawTpu} />
    ) : (
      accelerator.name
    );

  return (
    <Tooltip title={title}>
      <Box sx={{ display: "flex", minWidth: 120 }}>
        <RightPaddedTypography variant="body1">[{slot}]:</RightPaddedTypography>
        {accelerator.utilization !== undefined ? (
          <UsageBar
            percent={accelerator.utilization}
            text={`${accelerator.utilization.toFixed(1)}%`}
          />
        ) : (
          <Typography color="textSecondary" component="span" variant="inherit">
            N/A
          </Typography>
        )}
      </Box>
    </Tooltip>
  );
};

export const NodeGPUView = ({ node }: { node: NodeDetail }) => {
  const accelerators = normalizeAccelerators(node.gpus, node.tpus);

  return (
    <Box sx={{ minWidth: 120 }}>
      {accelerators.length !== 0 ? (
        accelerators.map((acc, i) => (
          <NodeGPUEntry
            key={acc.uuid || acc.name + acc.index}
            accelerator={acc}
            slot={acc.index}
          />
        ))
      ) : (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      )}
    </Box>
  );
};

export const WorkerGpuRow = ({
  workerPID,
  gpus,
  tpus,
}: {
  workerPID: number | null;
  gpus?: GPUStats[];
  tpus?: TPUStats[];
}) => {
  const accelerators = normalizeAccelerators(gpus, tpus);

  const workerGPUEntries = accelerators
    .map((acc, i) => {
      // TPUs currently do not report per-process PIDs, so we skip them for worker rows
      if (acc.type === "TPU") {
        return undefined;
      }
      const process = acc.processesPids?.find(
        (process) => process.pid === workerPID,
      );
      if (!process) {
        return undefined;
      }
      return (
        <NodeGPUEntry
          key={acc.uuid || acc.name + acc.index}
          accelerator={acc}
          slot={acc.index}
        />
      );
    })
    .filter((entry) => entry !== undefined);

  return workerGPUEntries.length === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      N/A
    </Typography>
  ) : (
    <Box sx={{ minWidth: 120 }}>{workerGPUEntries}</Box>
  );
};

export const getSumGpuUtilization = (
  workerPID: number | null,
  gpus?: GPUStats[],
  tpus?: TPUStats[],
) => {
  const accelerators = normalizeAccelerators(gpus, tpus);
  const workerGPUUtilizationEntries = accelerators
    .map((acc, i) => {
      const process = acc.processesPids?.find(
        (process) => process.pid === workerPID,
      );
      if (!process) {
        return 0;
      }
      return acc.utilization || 0;
    })
    .filter((entry) => entry !== undefined);
  return workerGPUUtilizationEntries.reduce((a, b) => a + b, 0);
};
