import { Box, SxProps, Theme, Typography } from "@mui/material";
import React, { useContext } from "react";
import { GlobalContext } from "../../../App";
import { GrafanaNotRunningAlert } from "../../metrics";
import { LinkWithArrow, OverviewCard } from "./OverviewCard";

type ClusterUtilizationCardProps = {
  className?: string;
  sx?: SxProps<Theme>;
};

export const ClusterUtilizationCard = ({
  className,
  sx,
}: ClusterUtilizationCardProps) => {
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
  const path = `/d-solo/${grafanaDefaultDashboardUid}/default-dashboard?orgId=1&theme=light&panelId=41&var-datasource=${dashboardDatasource}`;
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
      {/* TODO (aguo): Switch this to overall utilization graph */}
      {/* TODO (aguo): Handle grafana not running */}
      {grafanaHost === undefined || !prometheusHealth ? (
        <Box sx={{ flex: 1, paddingX: 3, paddingY: 2 }}>
          <Typography variant="h3">Cluster utilization</Typography>
          <GrafanaNotRunningAlert sx={{ marginTop: 2 }} severity="info" />
        </Box>
      ) : (
        <React.Fragment>
          <Box
            component="iframe"
            title="Cluster Utilization"
            sx={{ flex: 1 }}
            src={`${grafanaHost}${path}&refresh${timeRangeParams}&var-SessionName=${sessionName}`}
            frameBorder="0"
          />
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
            <LinkWithArrow text="View all metrics" to="/metrics" />
          </Box>
        </React.Fragment>
      )}
    </OverviewCard>
  );
};
