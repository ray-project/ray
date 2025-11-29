import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import PercentageBar from "../../components/PercentageBar";
import { GPUStats, NodeDetail } from "../../type/node";

const VRAM_COL_WIDTH = 120;

export const NodeVRAM = ({ node }: { node: NodeDetail }) => {
  const nodeVRAMEntries = (node.gpus ?? []).map((gpu, i) => {
    const props = {
      key: gpu.uuid,
      gpuName: gpu.name,
      utilization: gpu.memoryUsed,
      total: gpu.memoryTotal,
      slot: gpu.index,
    };
    return <VRAMEntry {...props} />;
  });
  return (
    <div style={{ minWidth: 60 }}>
      {nodeVRAMEntries.length === 0 ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      ) : (
        <div style={{ minWidth: VRAM_COL_WIDTH }}>{nodeVRAMEntries}</div>
      )}
    </div>
  );
};

export const WorkerVRAM = ({
  workerPID,
  gpus,
}: {
  workerPID: number | null;
  gpus?: GPUStats[];
}) => {
  const workerVRAMEntries = (gpus ?? [])
    .map((gpu, i) => {
      const process = gpu.processesPids?.find(
        (process) => workerPID && process.pid === workerPID,
      );
      if (!process) {
        return undefined;
      }
      const props = {
        key: gpu.uuid,
        gpuName: gpu.name,
        total: gpu.memoryTotal,
        utilization: process.gpuMemoryUsage,
        slot: gpu.index,
      };
      return <VRAMEntry {...props} />;
    })
    .filter((entry) => entry !== undefined);

  return workerVRAMEntries.length === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      N/A
    </Typography>
  ) : (
    <div style={{ minWidth: VRAM_COL_WIDTH }}>{workerVRAMEntries}</div>
  );
};

export const getSumVRAMUsage = (
  workerPID: number | null,
  gpus?: GPUStats[],
) => {
  // Get sum of all VRAM usage values for this worker PID. This is an
  // aggregate of WorkerVRAM and follows the same logic.
  const workerVRAMEntries = (gpus ?? [])
    .map((gpu, i) => {
      const process = gpu.processesPids?.find(
        (process) => workerPID && process.pid === workerPID,
      );
      if (!process) {
        return 0;
      }
      return process.gpuMemoryUsage;
    })
    .filter((entry) => entry !== undefined);
  return workerVRAMEntries.reduce((a, b) => a + b, 0);
};

const getMiBRatioNoPercent = (used: number, total: number) =>
  `${used}MiB/${total}MiB`;

type VRAMEntryProps = {
  gpuName: string;
  slot: number;
  utilization: number;
  total: number;
};

const VRAMEntry: React.FC<VRAMEntryProps> = ({
  gpuName,
  slot,
  utilization,
  total,
}) => {
  const ratioStr = getMiBRatioNoPercent(utilization, total);
  return (
    <Box display="flex" flexWrap="nowrap" style={{ minWidth: VRAM_COL_WIDTH }}>
      <Tooltip title={gpuName}>
        <Box display="flex" flexWrap="nowrap" flexGrow={1}>
          <RightPaddedTypography variant="body1">
            [{slot}]:{" "}
          </RightPaddedTypography>
          <PercentageBar num={utilization} total={total}>
            {ratioStr}
          </PercentageBar>
        </Box>
      </Tooltip>
    </Box>
  );
};
