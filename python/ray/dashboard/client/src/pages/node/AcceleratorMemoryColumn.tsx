import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import PercentageBar from "../../components/PercentageBar";
import { AcceleratorStats } from "../../type/accelerator";

const RESOURCE_MEMORY_COL_WIDTH = 120;

export const NodeAcceleratorMemory = ({
  accelerators,
}: {
  accelerators: AcceleratorStats[];
}) => {
  const nodeAcceleratorMemoryEntries = (accelerators ?? []).map(
    (accelerator, i) => {
      const props = {
        key: accelerator.uuid,
        resourceName: accelerator.name,
        utilization: accelerator.memoryUsed,
        total: accelerator.memoryTotal,
        slot: accelerator.index,
      };
      return <AcceleratorMemoryEntry {...props} />;
    },
  );
  return (
    <div style={{ minWidth: 60 }}>
      {nodeAcceleratorMemoryEntries.length === 0 ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      ) : (
        <div style={{ minWidth: RESOURCE_MEMORY_COL_WIDTH }}>
          {nodeAcceleratorMemoryEntries}
        </div>
      )}
    </div>
  );
};

export const WorkerAcceleratorMemory = ({
  workerPID,
  accelerators,
}: {
  workerPID: number | null;
  accelerators?: AcceleratorStats[];
}) => {
  const workerAcceleratorMemoryEntries = (accelerators ?? [])
    .map((accelerator, i) => {
      const process = accelerator.processes?.find(
        (process) => workerPID && process.pid === workerPID,
      );
      if (!process) {
        return undefined;
      }
      const props = {
        key: accelerator.uuid,
        resourceName: accelerator.name,
        total: accelerator.memoryTotal,
        utilization: process.memoryUsage,
        slot: accelerator.index,
      };
      return <AcceleratorMemoryEntry {...props} />;
    })
    .filter((entry) => entry !== undefined);

  return workerAcceleratorMemoryEntries.length === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      N/A
    </Typography>
  ) : (
    <div style={{ minWidth: RESOURCE_MEMORY_COL_WIDTH }}>
      {workerAcceleratorMemoryEntries}
    </div>
  );
};

export const getSumAcceleratorMemoryUsage = (
  workerPID: number | null,
  accelerators?: AcceleratorStats[],
) => {
  const workerAcceleratorMemoryEntries = (accelerators ?? [])
    .map((accelerator, i) => {
      const process = accelerator.processes?.find(
        (process) => workerPID && process.pid === workerPID,
      );
      if (!process) {
        return 0;
      }
      return process.memoryUsage;
    })
    .filter((entry) => entry !== undefined);
  return workerAcceleratorMemoryEntries.reduce((a, b) => a + b, 0);
};

const getMiBRatioNoPercent = (used: number, total: number) =>
  `${used}MiB/${total}MiB`;

type AcceleratorMemoryEntryProps = {
  resourceName: string;
  slot: number;
  utilization: number;
  total: number;
};

const AcceleratorMemoryEntry: React.FC<AcceleratorMemoryEntryProps> = ({
  resourceName,
  slot,
  utilization,
  total,
}) => {
  const ratioStr = getMiBRatioNoPercent(utilization, total);
  return (
    <Box
      display="flex"
      flexWrap="nowrap"
      style={{ minWidth: RESOURCE_MEMORY_COL_WIDTH }}
    >
      <Tooltip title={resourceName}>
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
