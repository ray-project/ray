import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import PercentageBar from "../../components/PercentageBar";
import { NodeDetail, NPUStats } from "../../type/node";

const HBM_COL_WIDTH = 120;

export const NodeHBM = ({ node }: { node: NodeDetail }) => {
  const nodeHBMEntries = (node.npus ?? []).map((npu, i) => {
    const props = {
      key: npu.uuid,
      npuName: npu.name,
      utilization: npu.memoryUsed,
      total: npu.memoryTotal,
      slot: npu.index,
    };
    return <HBMEntry {...props} />;
  });
  return (
    <div style={{ minWidth: 60 }}>
      {nodeHBMEntries.length === 0 ? (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      ) : (
        <div style={{ minWidth: HBM_COL_WIDTH }}>{nodeHBMEntries}</div>
      )}
    </div>
  );
};

export const WorkerHBM = ({
  workerPID,
  npus,
}: {
  workerPID: number | null;
  npus?: NPUStats[];
}) => {
  const workerHBMEntries = (npus ?? [])
    .map((npu, i) => {
      const process = npu.processes?.find(
        (process) => workerPID && process.pid === workerPID,
      );
      if (!process) {
        return undefined;
      }
      const props = {
        key: npu.uuid,
        npuName: npu.name,
        total: npu.memoryTotal,
        utilization: process.npuMemoryUsage,
        slot: npu.index,
      };
      return <HBMEntry {...props} />;
    })
    .filter((entry) => entry !== undefined);

  return workerHBMEntries.length === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      N/A
    </Typography>
  ) : (
    <div style={{ minWidth: HBM_COL_WIDTH }}>{workerHBMEntries}</div>
  );
};

export const getSumHBMUsage = (workerPID: number | null, npus?: NPUStats[]) => {
  // Get sum of all HBM usage values for this worker PID. This is an
  // aggregate of WorkerHBM and follows the same logic.
  const workerHBMEntries = (npus ?? [])
    .map((npu, i) => {
      const process = npu.processes?.find(
        (process) => workerPID && process.pid === workerPID,
      );
      if (!process) {
        return 0;
      }
      return process.npuMemoryUsage;
    })
    .filter((entry) => entry !== undefined);
  return workerHBMEntries.reduce((a, b) => a + b, 0);
};

const getMiBRatioNoPercent = (used: number, total: number) =>
  `${used}MiB/${total}MiB`;

type HBMEntryProps = {
  npuName: string;
  slot: number;
  utilization: number;
  total: number;
};

const HBMEntry: React.FC<HBMEntryProps> = ({
  npuName,
  slot,
  utilization,
  total,
}) => {
  const ratioStr = getMiBRatioNoPercent(utilization, total);
  return (
    <Box display="flex" flexWrap="nowrap" style={{ minWidth: HBM_COL_WIDTH }}>
      <Tooltip title={npuName}>
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
