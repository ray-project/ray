import { Box, Typography } from "@material-ui/core";
import React from "react";
import { RayStatusResp } from "../service/status";
import TitleCard from "./TitleCard";

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
      <Typography variant="h6">
        <b>{title}</b>
      </Typography>
      {cluster_status_rows.map((i, key) => {
        // Format the output.
        // See format_info_string in util.py
        if (i.startsWith("-----") || i.startsWith("=====")) {
          // Separator
          return <div key={key} />;
        } else if (i.endsWith(":")) {
          return (
            <div key={key}>
              <b>{i}</b>
            </div>
          );
        } else if (i === "") {
          return <br key={key} />;
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
    <TitleCard title="">
      <Box
        mb={2}
        display="flex"
        flexDirection="column"
        height="300px"
        style={{
          overflow: "hidden",
          overflowY: "scroll",
        }}
        sx={{ borderRadius: "16px" }}
      >
        {cluster_status?.data
          ? formatNodeStatus(cluster_status?.data.clusterStatus)
          : "No cluster status."}
      </Box>
    </TitleCard>
  );
};

export const ResourceStatusCard = ({ cluster_status }: StatusCardProps) => {
  return (
    <TitleCard title="">
      <Box
        mb={2}
        display="flex"
        flexDirection="column"
        height="300px"
        style={{
          overflow: "hidden",
          overflowY: "scroll",
        }}
        sx={{ border: 1, borderRadius: "1", borderColor: "primary.main" }}
      >
        {cluster_status?.data
          ? formatResourcesStatus(cluster_status?.data.clusterStatus)
          : "No cluster status."}
      </Box>
    </TitleCard>
  );
};
