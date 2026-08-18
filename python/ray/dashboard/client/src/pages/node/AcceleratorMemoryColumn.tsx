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

// Accelerators may report memory as null (GPUs with no separate memory pool, such
// as unified-memory parts) or NaN (TPU generations that omit the absolute
// metrics). Both mean "unknown" and must not be rendered as a quantity.
const isKnownMemoryValue = (
  value: number | null | undefined,
): value is number => value !== null && value !== undefined && !isNaN(value);

type AcceleratorMemoryEntryProps = {
  gpuName: string;
  slot: number;
  utilization: number | null;
  total: number | null;
  utilPercent?: number;
};

const AcceleratorMemoryEntry: React.FC<AcceleratorMemoryEntryProps> = ({
  gpuName,
  slot,
  utilization,
  total,
  utilPercent,
}) => {
  let used = utilization;
  let capacity = total;
  let ratioStr: string | undefined;

  // When the utilization percentage is present but absolute usage is missing
  // (as is the case on some TPU generations), spoof the bar with just a percentage.
  if (
    utilPercent !== undefined &&
    (capacity === 0 || !isKnownMemoryValue(capacity))
  ) {
    ratioStr = `${utilPercent.toFixed(1)}%`;
    used = utilPercent;
    capacity = 100;
  }

  let body: React.ReactNode;
  if (isKnownMemoryValue(used) && isKnownMemoryValue(capacity)) {
    body = (
      <PercentageBar num={used} total={capacity}>
        {ratioStr ?? getMemDisplayRatioNoPercent(used, capacity)}
      </PercentageBar>
    );
  } else if (isKnownMemoryValue(used)) {
    // Usage is known but capacity is not, so there is no ratio to draw. Show the
    // absolute usage rather than discarding a real measurement.
    body = (
      <Typography component="span" variant="inherit">
        {memoryConverter(used * 1024 * 1024)}
      </Typography>
    );
  } else {
    body = (
      <Typography color="textSecondary" component="span" variant="inherit">
        N/A
      </Typography>
    );
  }

  return (
    <Box display="flex" flexWrap="nowrap" style={{ minWidth: GRAM_COL_WIDTH }}>
      <Tooltip title={gpuName}>
        <Box display="flex" flexWrap="nowrap" flexGrow={1}>
          <RightPaddedTypography variant="body1">
            [{slot}]:{" "}
          </RightPaddedTypography>
          {body}
        </Box>
      </Tooltip>
    </Box>
  );
};
