import { Box, Tooltip, Typography } from "@material-ui/core";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import PercentageBar from "../../components/PercentageBar";
import { NodeDetail } from "../../type/node";
import { Worker } from "../../type/worker";

const GRAM_COL_WIDTH = 120;

export const NodeGRAM = ({ node }: { node: NodeDetail }) => {
  const nodeGRAMEntries = (node.gpus ?? []).map((gpu, i) => {
    const props = {
      key: gpu.uuid,
      gpuName: gpu.name,
      utilization: gpu.memoryUsed,
      total: gpu.memoryTotal,
      slot: i,
    };
    return <GRAMEntry {...props} />;
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

export const WorkerGRAM = ({
  worker,
  node,
}: {
  worker: Worker;
  node: NodeDetail;
}) => {
  const workerGRAMEntries = (node.gpus ?? [])
    .map((gpu, i) => {
      const process = gpu.processes?.find(
        (process) => process.pid === worker.pid,
      );
      if (!process) {
        return undefined;
      }
      const props = {
        key: gpu.uuid,
        gpuName: gpu.name,
        total: gpu.memoryTotal,
        utilization: process.gpuMemoryUsage,
        slot: i,
      };
      return <GRAMEntry {...props} />;
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

const getMiBRatioNoPercent = (used: number, total: number) =>
  `${used}MiB/${total}MiB`;

type GRAMEntryProps = {
  gpuName: string;
  slot: number;
  utilization: number;
  total: number;
};

const GRAMEntry: React.FC<GRAMEntryProps> = ({
  gpuName,
  slot,
  utilization,
  total,
}) => {
  const ratioStr = getMiBRatioNoPercent(utilization, total);
  return (
    <Box display="flex" flexWrap="nowrap" style={{ minWidth: GRAM_COL_WIDTH }}>
      <Tooltip title={gpuName}>
        <Box display="flex" flexWrap="nowrap">
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
