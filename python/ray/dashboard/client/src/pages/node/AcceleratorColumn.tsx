import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import UsageBar from "../../common/UsageBar";
import { GPUStats, NodeDetail, NPUStats, TPUStats } from "../../type/node";
import {
  normalizeAccelerators,
  UnifiedAcceleratorStat,
} from "../../util/accelerator";

export type NodeAcceleratorEntryProps = {
  slot: number;
  accelerator: UnifiedAcceleratorStat;
};

const GpuTooltip = ({ gpu }: { gpu: GPUStats }) => {
  return (
    <Box>
      <Typography variant="body2">Name: {gpu.name}</Typography>
      {gpu.temperatureC !== undefined && gpu.temperatureC !== null && (
        <Typography variant="body2">
          Temperature: {gpu.temperatureC}°C
        </Typography>
      )}
      {gpu.powerMw !== undefined && gpu.powerMw !== null && (
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
        HBM Utilization: {hbmUtilization.toFixed(1)}%
      </Typography>
    </Box>
  );
};

const NPU_HEALTH_LABELS: Record<number, string> = {
  0: "Healthy",
  1: "Minor",
  2: "Major",
  3: "Critical",
  4: "Missing",
};

const NpuTooltip = ({ npu }: { npu: NPUStats }) => {
  return (
    <Box>
      <Typography variant="body2">Name: {npu.name}</Typography>
      <Typography variant="body2">
        Card/Device: {npu.cardId}/{npu.deviceId}
      </Typography>
      {npu.physicalId !== undefined && npu.physicalId !== null && (
        <Typography variant="body2">Physical ID: {npu.physicalId}</Typography>
      )}
      {npu.healthStatus !== undefined && npu.healthStatus !== null && (
        <Typography variant="body2">
          Health:{" "}
          {NPU_HEALTH_LABELS[npu.healthStatus] ??
            `Unknown (${npu.healthStatus})`}
        </Typography>
      )}
      {npu.temperatureC !== undefined && npu.temperatureC !== null && (
        <Typography variant="body2">
          Temperature: {npu.temperatureC}°C
        </Typography>
      )}
      {npu.powerMw !== undefined && npu.powerMw !== null && (
        <Typography variant="body2">
          Power Draw: {(npu.powerMw / 1000).toFixed(1)} W
        </Typography>
      )}
    </Box>
  );
};

export const NodeAcceleratorEntry: React.FC<NodeAcceleratorEntryProps> = ({
  accelerator,
  slot,
}) => {
  const title =
    accelerator.type === "GPU" && accelerator.rawGpu ? (
      <GpuTooltip gpu={accelerator.rawGpu} />
    ) : accelerator.type === "NPU" && accelerator.rawNpu ? (
      <NpuTooltip npu={accelerator.rawNpu} />
    ) : accelerator.type === "TPU" && accelerator.rawTpu ? (
      <TpuTooltip tpu={accelerator.rawTpu} />
    ) : (
      accelerator.name
    );

  return (
    <Tooltip title={title}>
      <Box sx={{ display: "flex", minWidth: 120 }}>
        <RightPaddedTypography variant="body1">[{slot}]:</RightPaddedTypography>
        {accelerator.utilization !== undefined &&
        accelerator.utilization !== null ? (
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

export const NodeAcceleratorView = ({ node }: { node: NodeDetail }) => {
  const accelerators = normalizeAccelerators(node.gpus, node.tpus, node.npus);

  return (
    <Box sx={{ minWidth: 120 }}>
      {accelerators.length !== 0 ? (
        accelerators.map((acc, i) => (
          <NodeAcceleratorEntry
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

export const WorkerAcceleratorRow = ({
  workerPID,
  gpus,
  npus,
  tpus,
}: {
  workerPID: number | null;
  gpus?: GPUStats[];
  npus?: NPUStats[];
  tpus?: TPUStats[];
}) => {
  const accelerators = normalizeAccelerators(gpus, tpus, npus);

  const workerGPUEntries = accelerators
    .map((acc, i) => {
      const process = acc.processesPids?.find(
        (process) => process.pid === workerPID,
      );
      if (!process) {
        return undefined;
      }
      return (
        <NodeAcceleratorEntry
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

export const getSumAcceleratorUtilization = (
  workerPID: number | null,
  gpus?: GPUStats[],
  tpus?: TPUStats[],
  npus?: NPUStats[],
) => {
  const accelerators = normalizeAccelerators(gpus, tpus, npus);
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
