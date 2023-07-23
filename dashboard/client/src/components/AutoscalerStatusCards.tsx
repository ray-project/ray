import { Box, Typography } from "@material-ui/core";
import React from "react";
import { RayStatusResp } from "../service/status";

const formatNodeStatus = (cluster_status: string) => {
  // ==== auto scaling status
  // Node status
  // ....
  // Resources
  // ....
  const sections = cluster_status.split("Resources");
  return formatClusterStatus(
    "Node Status",
    sections[0].split("Node status")[1],
  );
};

const formatResourcesStatus = (cluster_status: string) => {
  // ==== auto scaling status
  // Node status
  // ....
  // Resources
  // ....
  const sections = cluster_status.split("Resources");
  return formatClusterStatus("Resource Status", sections[1]);
};

const formatClusterStatus = (title: string, cluster_status: string) => {
  const cluster_status_rows = cluster_status.split("\n");

  return (
    <div>
      <Box marginBottom={2}>
        <Typography variant="h3">{title}</Typography>
      </Box>
      {cluster_status_rows.map((i, key) => {
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
  cluster_status: RayStatusResp | undefined;
};

export const NodeStatusCard = ({ cluster_status }: StatusCardProps) => {
  return (
    <Box
      style={{
        overflow: "hidden",
        overflowY: "scroll",
      }}
    >
      {cluster_status?.data
        ? formatNodeStatus(cluster_status?.data.clusterStatus)
        : "No cluster status."}
    </Box>
  );
};

export const ResourceStatusCard = ({ cluster_status }: StatusCardProps) => {
  return (
    <Box
      style={{
        overflow: "hidden",
        overflowY: "scroll",
      }}
    >
      {cluster_status?.data
        ? formatResourcesStatus(cluster_status?.data.clusterStatus)
        : "No cluster status."}
    </Box>
  );
};
