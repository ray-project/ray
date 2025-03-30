import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import UsageBar from "../../common/UsageBar";
import { AcceleratorStats } from "../../type/accelerator";

export type NodeAcceleratorEntryProps = {
  slot: number;
  accelerator: AcceleratorStats;
};

export const NodeAcceleratorEntry: React.FC<NodeAcceleratorEntryProps> = ({
  slot,
  accelerator,
}) => {
  return (
    <Tooltip title={accelerator.name}>
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

export const NodeAcceleratorUtilization = ({
  accelerators,
}: {
  accelerators: AcceleratorStats[];
}) => {
  return (
    <Box sx={{ minWidth: 120 }}>
      {accelerators !== undefined && accelerators.length !== 0 ? (
        accelerators.map((accelerator, i) => (
          <NodeAcceleratorEntry
            key={accelerator.uuid}
            accelerator={accelerator}
            slot={accelerator.index}
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

export const WorkerAcceleratorUtilization = ({
  workerPID,
  accelerators,
}: {
  workerPID: number | null;
  accelerators?: AcceleratorStats[];
}) => {
  const workerAcceleratorEntries = (accelerators ?? [])
    .map((accelerator, i) => {
      const process = accelerator.processes?.find(
        (process) => process.pid === workerPID,
      );
      if (!process) {
        return undefined;
      }
      return (
        <NodeAcceleratorEntry
          key={accelerator.uuid}
          accelerator={accelerator}
          slot={accelerator.index}
        />
      );
    })
    .filter((entry) => entry !== undefined);

  return workerAcceleratorEntries.length === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      N/A
    </Typography>
  ) : (
    <Box sx={{ minWidth: 120 }}>{workerAcceleratorEntries}</Box>
  );
};

export const getSumAcceleratorUtilization = (
  workerPID: number | null,
  accelerators?: AcceleratorStats[],
) => {
  const workerAcceleratorUtilizationEntries = (accelerators ?? [])
    .map((accelerator, i) => {
      const process = accelerator.processes?.find(
        (process) => process.pid === workerPID,
      );
      if (!process) {
        return 0;
      }
      return accelerator.utilization || 0;
    })
    .filter((entry) => entry !== undefined);
  return workerAcceleratorUtilizationEntries.reduce((a, b) => a + b, 0);
};
