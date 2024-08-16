import { Box, Typography } from "@mui/material";
import React from "react";
import { RayStatusResp } from "../service/status";

const formatNodeStatus = (clusterStatus?: string) => {
  // ==== auto scaling status
  // Node status
  // ....
  // Resources
  // ....
  if (!clusterStatus) {
    return "No cluster status.";
  }
  try {
    // Try to parse the node status.
    const sections = clusterStatus.split("Resources");
    return formatClusterStatus(
      "Node Status",
      sections[0].split("Node status")[1],
    );
  } catch (e) {
    return "No cluster status.";
  }
};

const formatResourcesStatus = (clusterStatus?: string) => {
  // ==== auto scaling status
  // Node status
  // ....
  // Resources
  // ....
  if (!clusterStatus) {
    return "No cluster status.";
  }
  try {
    const sections = clusterStatus.split("Resources");
    return formatClusterStatus("Resource Status", sections[1]);
  } catch (e) {
    return "No cluster status.";
  }
};

const formatClusterStatus = (title: string, clusterStatus: string) => {
  const clusterStatusRows = clusterStatus.split("\n");

  return (
    <div>
      <Box marginBottom={2}>
        <Typography variant="h3">{title}</Typography>
      </Box>
      {clusterStatusRows.map((i, key) => {
        // Format the output.
        // See format_info_string in util.py
        if (i.startsWith("-----") || i.startsWith("=====") || i === "") {
          // Ignore separators
          return null;
        } else if (i.endsWith(":")) {
          return (
            <div key={key}>
              <b>{i}</b>
            </div>
          );
        } else {
          return <div key={key}>{i}</div>;
        }
      })}
    </div>
  );
};

type StatusCardProps = {
  clusterStatus: RayStatusResp | undefined;
};

export const NodeStatusCard = ({ clusterStatus }: StatusCardProps) => {
  return (
    <Box
      style={{
        overflow: "hidden",
        overflowY: "scroll",
      }}
    >
      {formatNodeStatus(clusterStatus?.data.clusterStatus)}
    </Box>
  );
};

export const ResourceStatusCard = ({ clusterStatus }: StatusCardProps) => {
  return (
    <Box
      style={{
        overflow: "hidden",
        overflowY: "scroll",
      }}
    >
      {formatResourcesStatus(clusterStatus?.data.clusterStatus)}
    </Box>
  );
};
