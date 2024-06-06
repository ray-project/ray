import { Box, styled, Tooltip, Typography } from "@mui/material";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import UsageBar from "../../common/UsageBar";
import { GPUStats, NodeDetail } from "../../type/node";

const StyledBox = styled(Box)(({ theme }) => ({
  display: "flex",
  minWidth: 120,
}));

const GpuColumnDiv = styled("div")(({ theme }) => ({
  minWidth: 120,
}));

export type NodeGPUEntryProps = {
  slot: number;
  gpu: GPUStats;
};

export const NodeGPUEntry: React.FC<NodeGPUEntryProps> = ({ gpu, slot }) => {
  return (
    <Tooltip title={gpu.name}>
      <StyledBox>
        <RightPaddedTypography variant="body1">[{slot}]:</RightPaddedTypography>
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
      </StyledBox>
    </Tooltip>
  );
};

export const NodeGPUView = ({ node }: { node: NodeDetail }) => {
  return (
    <GpuColumnDiv>
      {node.gpus !== undefined && node.gpus.length !== 0 ? (
        node.gpus.map((gpu, i) => (
          <NodeGPUEntry key={gpu.uuid} gpu={gpu} slot={gpu.index} />
        ))
      ) : (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      )}
    </GpuColumnDiv>
  );
};

export const WorkerGpuRow = ({
  workerPID,
  gpus,
}: {
  workerPID: number | null;
  gpus?: GPUStats[];
}) => {
  const workerGPUEntries = (gpus ?? [])
    .map((gpu, i) => {
      const process = gpu.processes?.find(
        (process) => process.pid === workerPID,
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
    <GpuColumnDiv>{workerGPUEntries}</GpuColumnDiv>
  );
};

export const getSumGpuUtilization = (
  workerPID: number | null,
  gpus?: GPUStats[],
) => {
  // Get sum of all GPU utilization values for this worker PID. This is an
  // aggregate of the WorkerGpuRow and follows the same logic.
  const workerGPUUtilizationEntries = (gpus ?? [])
    .map((gpu, i) => {
      const process = gpu.processes?.find(
        (process) => process.pid === workerPID,
      );
      if (!process) {
        return 0;
      }
      return gpu.utilizationGpu || 0;
    })
    .filter((entry) => entry !== undefined);
  return workerGPUUtilizationEntries.reduce((a, b) => a + b, 0);
};
