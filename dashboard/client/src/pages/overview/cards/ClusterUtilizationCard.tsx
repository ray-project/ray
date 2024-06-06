import { Typography } from "@mui/material";
import { styled } from "@mui/material/styles";
import React, { useContext } from "react";
import { GlobalContext } from "../../../App";
import { GrafanaNotRunningAlert } from "../../metrics";
import { LinkWithArrow, OverviewCard } from "./OverviewCard";

const RootOverviewCard = styled(OverviewCard)(({theme}) => ({ 
  display: "flex",
  flexDirection: "column",
  flexWrap: "nowrap",
}));

const NoGraphDiv = styled("div")(({theme}) => ({ 
  flex: 1,
  padding: theme.spacing(2, 3),
}));

const GraphIFrame = styled("iframe")(({theme}) => ({ 
  flex: 1,
}));

const StyledGrafanaNotRunningAlert = styled(GrafanaNotRunningAlert)(({theme}) => ({ 
  marginTop: theme.spacing(2),
}));

const LinksDiv = styled("div")(({theme}) => ({ 
  display: "flex",
  flexDirection: "row",
  flexWrap: "nowrap",
  margin: theme.spacing(1, 3, 2),
}));

type ClusterUtilizationCardProps = {
  className?: string;
};

export const ClusterUtilizationCard = ({
  className,
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
    <RootOverviewCard className={className}>
      {/* TODO (aguo): Switch this to overall utilization graph */}
      {/* TODO (aguo): Handle grafana not running */}
      {grafanaHost === undefined || !prometheusHealth ? (
        <NoGraphDiv>
          <Typography variant="h3">Cluster utilization</Typography>
          <StyledGrafanaNotRunningAlert severity="info" />
        </NoGraphDiv>
      ) : (
        <React.Fragment>
          <GraphIFrame
            title="Cluster Utilization"
            src={`${grafanaHost}${path}&refresh${timeRangeParams}&var-SessionName=${sessionName}`}
            frameBorder="0"
          />
          <LinksDiv>
            <LinkWithArrow text="View all metrics" to="/metrics" />
          </LinksDiv>
        </React.Fragment>
      )}
    </RootOverviewCard>
  );
};
