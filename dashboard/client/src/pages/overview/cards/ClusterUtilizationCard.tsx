import { Box, SxProps, Theme, Typography, useTheme } from "@mui/material";
import React, { useContext } from "react";
import { GlobalContext } from "../../../App";
import { GrafanaNotRunningAlert } from "../../metrics";
import { LinkWithArrow, OverviewCard } from "./OverviewCard";

const useStyles = (theme: Theme) => ({
  root: {
    display: "flex",
    flexDirection: "column",
    flexWrap: "nowrap",
  },
  graph: {
    flex: 1,
  },
  noGraph: {
    flex: 1,
    padding: theme.spacing(2, 3),
  },
  alert: {
    marginTop: theme.spacing(2),
  },
  links: {
    display: "flex",
    flexDirection: "row",
    flexWrap: "nowrap",
    margin: theme.spacing(1, 3, 2),
  },
});

type ClusterUtilizationCardProps = {
  className?: string;
  sx?: SxProps<Theme>;
};

export const ClusterUtilizationCard = ({
  className,
  sx,
}: ClusterUtilizationCardProps) => {
  const styles = useStyles(useTheme());

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
    <OverviewCard className={className} sx={Object.assign({}, styles.root, sx)}>
      {/* TODO (aguo): Switch this to overall utilization graph */}
      {/* TODO (aguo): Handle grafana not running */}
      {grafanaHost === undefined || !prometheusHealth ? (
        <Box sx={styles.noGraph}>
          <Typography variant="h3">Cluster utilization</Typography>
          <GrafanaNotRunningAlert sx={styles.alert} severity="info" />
        </Box>
      ) : (
        <React.Fragment>
          <Box
            component="iframe"
            title="Cluster Utilization"
            sx={styles.graph}
            src={`${grafanaHost}${path}&refresh${timeRangeParams}&var-SessionName=${sessionName}`}
            frameBorder="0"
          />
          <Box sx={styles.links}>
            <LinkWithArrow text="View all metrics" to="/metrics" />
          </Box>
        </React.Fragment>
      )}
    </OverviewCard>
  );
};
