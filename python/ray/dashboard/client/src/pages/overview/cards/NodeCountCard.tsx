import { Box, SxProps, Theme, Typography } from "@mui/material";
import React, { useContext } from "react";
import { GlobalContext } from "../../../App";
import { GrafanaNotRunningAlert } from "../../metrics";
import { LinkWithArrow, OverviewCard } from "./OverviewCard";

type NodeCountCardProps = {
  className?: string;
  sx?: SxProps<Theme>;
};

export const NodeCountCard = ({ className, sx }: NodeCountCardProps) => {
  const {
    metricsContextLoaded,
    grafanaHost,
    prometheusHealth,
    sessionName,
    dashboardUids,
    dashboardDatasource,
  } = useContext(GlobalContext);
  const grafanaDefaultDashboardUid =
    dashboardUids?.default ?? "rayDefaultDashboard";
  const path = `/d-solo/${grafanaDefaultDashboardUid}/default-dashboard?orgId=1&theme=light&panelId=24&var-datasource=${dashboardDatasource}`;
  const timeRangeParams = "&from=now-30m&to=now";

  if (!metricsContextLoaded || grafanaHost === "DISABLED") {
    return null;
  }

  return (
    <OverviewCard
      className={className}
      sx={[
        { display: "flex", flexDirection: "column", flexWrap: "nowrap" },
        ...(Array.isArray(sx) ? sx : [sx]),
      ]}
    >
      {grafanaHost === undefined || !prometheusHealth ? (
        <Box sx={{ flex: 1, paddingX: 3, paddingY: 2 }}>
          <Typography variant="h3">Node count</Typography>
          <GrafanaNotRunningAlert sx={{ marginTop: 2 }} severity="info" />
        </Box>
      ) : (
        <Box
          component="iframe"
          title="Node Count"
          sx={{ flex: 1 }}
          src={`${grafanaHost}${path}&refresh${timeRangeParams}&var-SessionName=${sessionName}`}
          frameBorder="0"
        />
      )}
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          flexWrap: "nowrap",
          marginX: 3,
          marginTop: 1,
          marginBottom: 2,
        }}
      >
        <LinkWithArrow text="View all nodes" to="/cluster" />
      </Box>
    </OverviewCard>
  );
};
