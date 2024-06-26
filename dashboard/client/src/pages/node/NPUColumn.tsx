import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import UsageBar from "../../common/UsageBar";
import { NodeDetail, NPUStats } from "../../type/node";

export const NodeNPUEntry: React.FC<NodeNPUEntryProps> = ({ npu, slot }) => {
  return (
    <Tooltip title={npu.name}>
      <Box sx={{ display: "flex", minWidth: 120 }}>
        <RightPaddedTypography variant="body1">[{slot}]:</RightPaddedTypography>
        {npu.utilizationNpu !== undefined ? (
          <UsageBar
            percent={npu.utilizationNpu}
            text={`${npu.utilizationNpu.toFixed(1)}%`}
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

export const NodeNPUView = ({ node }: { node: NodeDetail }) => {
  return (
    <Box sx={{ minWidth: 120 }}>
      {node.npus !== undefined && node.npus.length !== 0 ? (
        node.npus.map((npu, i) => (
          <NodeNPUEntry key={npu.uuid} npu={npu} slot={npu.index} />
        ))
      ) : (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      )}
    </Box>
  );
};

export const WorkerNpuRow = ({
  workerPID,
  npus,
}: {
  workerPID: number | null;
  npus?: NPUStats[];
}) => {
  const workerNPUEntries = (npus ?? [])
    .map((npu, i) => {
      const process = npu.processes?.find(
        (process) => process.pid === workerPID,
      );
      if (!process) {
        return undefined;
      }
      return <NodeNPUEntry key={npu.uuid} npu={npu} slot={npu.index} />;
    })
    .filter((entry) => entry !== undefined);

  return workerNPUEntries.length === 0 ? (
    <Typography color="textSecondary" component="span" variant="inherit">
      N/A
    </Typography>
  ) : (
    <Box sx={{ minWidth: 120 }}>{workerNPUEntries}</Box>
  );
};

export const getSumNpuUtilization = (
  workerPID: number | null,
  npus?: NPUStats[],
) => {
  // Get sum of all GPU utilization values for this worker PID. This is an
  // aggregate of the WorkerGpuRow and follows the same logic.
  const workerNPUUtilizationEntries = (npus ?? [])
    .map((npu, i) => {
      const process = npu.processes?.find(
        (process) => process.pid === workerPID,
      );
      if (!process) {
        return 0;
      }
      return npu.utilizationNpu || 0;
    })
    .filter((entry) => entry !== undefined);
  return workerNPUUtilizationEntries.reduce((a, b) => a + b, 0);
};
