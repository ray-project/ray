import { Box, Tooltip, Typography } from "@mui/material";
import makeStyles from '@mui/styles/makeStyles';
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import UsageBar from "../../common/UsageBar";
import { GPUStats, NodeDetail } from "../../type/node";
import { Worker } from "../../type/worker";

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
  worker,
  node,
}: {
  worker: Worker;
  node: NodeDetail;
}) => {
  const classes = useStyles();
  const workerGPUEntries = (node.gpus ?? [])
    .map((gpu, i) => {
      const process = gpu.processes?.find(
        (process) => process.pid === worker.pid,
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
