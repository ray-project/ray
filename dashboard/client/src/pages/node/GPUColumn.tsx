import { Box, Tooltip, Typography } from "@mui/material";
import makeStyles from "@mui/styles/makeStyles";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import UsageBar from "../../common/UsageBar";
import { GPUStats, NodeDetail } from "../../type/node";

const useStyles = makeStyles((theme) => ({
  gpuColumn: {
    minWidth: 120,
  },
  box: {
    display: "flex",
    minWidth: 120,
  },
}));

export type NodeGPUEntryProps = {
  slot: number;
  gpu: GPUStats;
};

export const NodeGPUEntry: React.FC<NodeGPUEntryProps> = ({ gpu, slot }) => {
  const classes = useStyles();
  return (
    <Tooltip title={gpu.name}>
      <Box className={classes.box}>
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
      </Box>
    </Tooltip>
  );
};

export const NodeGPUView = ({ node }: { node: NodeDetail }) => {
  const classes = useStyles();
  return (
    <div className={classes.gpuColumn}>
      {node.gpus !== undefined && node.gpus.length !== 0 ? (
        node.gpus.map((gpu, i) => (
          <NodeGPUEntry key={gpu.uuid} gpu={gpu} slot={gpu.index} />
        ))
      ) : (
        <Typography color="textSecondary" component="span" variant="inherit">
          N/A
        </Typography>
      )}
    </div>
  );
};

export const WorkerGpuRow = ({
  workerPID,
  gpus,
}: {
  workerPID: number | null;
  gpus?: GPUStats[];
}) => {
  const classes = useStyles();
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
    <div className={classes.gpuColumn}>{workerGPUEntries}</div>
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
