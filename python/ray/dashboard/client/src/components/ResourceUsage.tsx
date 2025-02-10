import { Box, Tooltip, Typography } from "@mui/material";
import React from "react";
import PercentageBar from "./PercentageBar";

const normalizeMemoryToBytes = (memoryStr: string): number => {
  const match = memoryStr.match(/([\d.]+)\s*([KMGT]?B)?/i);
  if (!match) {
    return 0;
  }

  const value = parseFloat(match[1]);
  const unit = (match[2] || "B").toUpperCase();

  const multipliers: Record<string, number> = {
    B: 1,
    KB: 1024,
    MB: 1024 ** 2,
    GB: 1024 ** 3,
    TB: 1024 ** 4,
  };

  return value * (multipliers[unit] || 1);
};

export const ResourceUsage = ({
  usageStr,
  resourceName,
}: {
  usageStr: string;
  resourceName: string;
}) => {
  const [usedStr, totalStr] = usageStr.split("/");

  const used = usedStr.toLowerCase().includes("b")
    ? normalizeMemoryToBytes(usedStr.trim())
    : parseFloat(usedStr.trim().split(" ")[0]);

  const total = totalStr.toLowerCase().includes("b")
    ? normalizeMemoryToBytes(totalStr.trim())
    : parseFloat(totalStr.trim().split(" ")[0]);

  const percent = total > 0 ? (used / total) * 100 : 0;

  return (
    <Box sx={{ width: "100%" }}>
      <PercentageBar num={used} total={total}>
        <Tooltip title={resourceName} enterDelay={0} leaveDelay={0} arrow>
          <Typography
            variant="caption"
            sx={{
              fontSize: "0.8rem",
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
              lineHeight: 1.2,
              width: "100%",
              height: "100%",
            }}
          >
            <span
              style={{
                display: "inline-block",
                maxWidth: "80px",
                whiteSpace: "nowrap",
                overflow: "hidden",
                textOverflow: "ellipsis",
                verticalAlign: "middle",
              }}
            >
              {resourceName}
            </span>
            {`: ${usedStr.trim()}/${totalStr.trim()} (${percent.toFixed(1)}%)`}
          </Typography>
        </Tooltip>
      </PercentageBar>
    </Box>
  );
};
