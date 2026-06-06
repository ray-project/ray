import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
import { RightPaddedTypography } from "../../common/CustomTypography";
import PercentageBar from "../../components/PercentageBar";
import { GPUStats, NodeDetail, TPUStats } from "../../type/node";
import {
  normalizeAccelerators,
} from "../../util/accelerator";

const GRAM_COL_WIDTH = 120;

export const NodeGRAM = ({ node }: { node: NodeDetail }) => {
  const accelerators = normalizeAccelerators(node.gpus, node.tpus);

  const nodeGRAMEntries = accelerators.map((acc, i) => {
    const props = {
      key: acc.uuid || acc.name + acc.index,
      gpuName: acc.name, // Displaying original name is fine, tooltip will use it. Or we could customize tooltip.
      utilization: acc.memoryUsed,
      total: acc.memoryTotal,
      slot: acc.index,
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
      // TPUs currently do not report per-process PIDs, so we skip them for worker rows
      if (acc.type === "TPU") {
        return undefined;
      }
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
        utilization: process.memoryUsage, // This is already unified
        slot: acc.index,
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

export const getSumGRAMUsage = (
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
  let unit = "MiB"
  if (total >= 1024) {
    used /= 1024
    total /= 1024
    unit = "GiB"
  }
  return `${used.toFixed(1)}${unit}/${total.toFixed(1)}${unit}`;
};

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
  const ratioStr = getMemDisplayRatioNoPercent(utilization, total);
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
