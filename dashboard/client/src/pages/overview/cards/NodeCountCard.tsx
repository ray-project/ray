import { Typography } from "@mui/material";
import { styled } from "@mui/material/styles";
import React, { useContext } from "react";
import { GlobalContext } from "../../../App";
import { GrafanaNotRunningAlert } from "../../metrics";
import { LinkWithArrow, OverviewCard } from "./OverviewCard";

const NodeCountCardRoot = styled(OverviewCard)(({theme}) => ({ 
  display: "flex",
  flexDirection: "column",
  flexWrap: "nowrap",
}));

const GraphIFrame = styled("iframe")(({theme}) => ({ 
  flex: 1,
}));

const NoGraphDiv = styled("div")(({theme}) => ({ 
  flex: 1,
  padding: theme.spacing(2, 3),
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

type NodeCountCardProps = {
  className?: string;
};

export const NodeCountCard = ({ className }: NodeCountCardProps) => {
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
    <NodeCountCardRoot className={className}>
      {grafanaHost === undefined || !prometheusHealth ? (
        <NoGraphDiv>
          <Typography variant="h3">Node count</Typography>
          <StyledGrafanaNotRunningAlert severity="info" />
        </NoGraphDiv>
      ) : (
        <GraphIFrame
          title="Node Count"
          src={`${grafanaHost}${path}&refresh${timeRangeParams}&var-SessionName=${sessionName}`}
          frameBorder="0"
        />
      )}
      <LinksDiv>
        <LinkWithArrow text="View all nodes" to="/cluster" />
      </LinksDiv>
    </NodeCountCardRoot>
  );
};
