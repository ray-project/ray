import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import PercentageBar from "../../components/PercentageBar";
import { GPUStats, NodeDetail, TPUStats } from "../../type/node";
import { normalizeAccelerators } from "../../util/accelerator";
import { memoryConverter } from "../../util/converter";

const GRAM_COL_WIDTH = 120;

export const NodeAcceleratorMemory = ({ node }: { node: NodeDetail }) => {
  const accelerators = normalizeAccelerators(node.gpus, node.tpus);

  const nodeGRAMEntries = accelerators.map((acc, i) => {
    const props = {
      key: acc.uuid || acc.name + acc.index,
      gpuName: acc.name,
      utilization: acc.memoryUsed,
      total: acc.memoryTotal,
      slot: acc.index,
      // TPUs report a raw ratio which is present even in cases where the
      // absolute usage is not available.
      utilPercent: acc.rawTpu ? acc.rawTpu.hbmUtilization : undefined,
    };
    return <AcceleratorMemoryEntry {...props} />;
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

export const WorkerAcceleratorMemory = ({
  workerPID,
  gpus,
  tpus,
}: {
  workerPID: number | null;
  gpus?: GPUStats[];
  tpus?: TPUStats[];
}) => {
  const accelerators = normalizeAccelerators(gpus, tpus);

  const workerGRAMEntries = accelerators
    .map((acc, i) => {
      const process = acc.processesPids?.find(
        (process) => workerPID && process.pid === workerPID,
      );
      if (!process) {
        return undefined;
      }
      const props = {
        key: acc.uuid || acc.name + acc.index,
        gpuName: acc.name,
        total: acc.memoryTotal,
        utilization: process.memoryUsage,
        slot: acc.index,
        // TPUs report a raw ratio which is present even in cases where the
        // absolute usage is not available.
        utilPercent: acc.rawTpu ? acc.rawTpu.hbmUtilization : undefined,
      };
      return <AcceleratorMemoryEntry {...props} />;
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

export const getSumAcceleratorMemoryUsage = (
  workerPID: number | null,
  gpus?: GPUStats[],
  tpus?: TPUStats[],
) => {
  const accelerators = normalizeAccelerators(gpus, tpus);

  const workerGRAMEntries = accelerators
    .map((acc, i) => {
      const process = acc.processesPids?.find(
        (process) => workerPID && process.pid === workerPID,
      );
      if (!process) {
        return 0;
      }
      return process.memoryUsage;
    })
    .filter((entry) => entry !== undefined);
  return workerGRAMEntries.reduce((a, b) => a + b, 0);
};

const getMemDisplayRatioNoPercent = (used: number, total: number) => {
  // Convert MiB back to bytes for the memoryConverter
  const usedBytes = used * 1024 * 1024;
  const totalBytes = total * 1024 * 1024;
  return `${memoryConverter(usedBytes)}/${memoryConverter(totalBytes)}`;
};

type AcceleratorMemoryEntryProps = {
  gpuName: string;
  slot: number;
  utilization: number;
  total: number;
  utilPercent?: number;
};

const AcceleratorMemoryEntry: React.FC<AcceleratorMemoryEntryProps> = ({
  gpuName,
  slot,
  utilization,
  total,
  utilPercent,
}) => {
  let ratioStr = getMemDisplayRatioNoPercent(utilization, total);
  // When the utilization percentage is present but absolute usage is missing
  // (as is the case on some TPU generations), spoof the bar with just a percentage.
  if (utilPercent !== undefined && (total === 0 || isNaN(total))) {
    ratioStr = `${utilPercent.toFixed(1)}%`;
    utilization = utilPercent;
    total = 100;
  }
  return (
    <Box display="flex" flexWrap="nowrap" style={{ minWidth: GRAM_COL_WIDTH }}>
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
