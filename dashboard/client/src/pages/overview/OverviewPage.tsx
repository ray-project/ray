import { styled, Theme } from "@mui/material/styles";
import React from "react";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import {
  NodeStatusCard,
  ResourceStatusCard,
} from "../../components/AutoscalerStatusCards";
import EventTable from "../../components/EventTable";
import { useRayStatus } from "../job/hook/useClusterStatus";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { ClusterUtilizationCard } from "./cards/ClusterUtilizationCard";
import { NodeCountCard } from "./cards/NodeCountCard";
import { OverviewCard } from "./cards/OverviewCard";
import { RecentJobsCard } from "./cards/RecentJobsCard";
import { RecentServeCard } from "./cards/RecentServeCard";

const overviewCardStyle = (theme: Theme) => ({
    flex: "1 0 448px",
    maxWidth: "100%",
    [theme.breakpoints.up("md")]: {
      // Calculate max width based on 1/3 of the total width minus padding between cards
      maxWidth: `calc((100% - ${theme.spacing(3)} * 2) / 3)`,
    },
  });

const RootDiv = styled("div")(({theme}) => ({
  padding: theme.spacing(3),
  backgroundColor: "white",
}));

const OverviewCardsContainer = styled("div")(({theme}) => ({
  display: "flex",
  flexDirection: "row",
  flexWrap: "wrap",
  marginBottom: theme.spacing(4),
  gap: theme.spacing(3),
  [theme.breakpoints.up("md")]: {
    flexWrap: "nowrap",
  },
}));

const StyledCollapsibleSection = styled(CollapsibleSection)(({theme}) => ({
  marginTop: theme.spacing(4),
}));

const StyledClusterUtilizationCard = styled(ClusterUtilizationCard)(({theme}) => ({
  ...overviewCardStyle(theme),
}));

const StyledRecentJobsCard = styled(RecentJobsCard)(({theme}) => ({
  ...overviewCardStyle(theme),
}));

const StyledRecentServeCard = styled(RecentServeCard)(({theme}) => ({
  ...overviewCardStyle(theme),
}));

const StyledNodeCountCard = styled(NodeCountCard)(({theme}) => ({
  ...overviewCardStyle(theme),
}));

const StyledOverviewCard = styled(OverviewCard)(({theme}) => ({
  backgroundColor: "white",
  ...overviewCardStyle(theme),
  padding: theme.spacing(2, 3),
}));

export const OverviewPage = () => {
  const { clusterStatus } = useRayStatus();
  return (
    <RootDiv>
      <MainNavPageInfo
        pageInfo={{ title: "Overview", id: "overview", path: "/overview" }}
      />
      <OverviewCardsContainer>
        <StyledClusterUtilizationCard />
        <StyledRecentJobsCard />
        <StyledRecentServeCard />
      </OverviewCardsContainer>

      <StyledCollapsibleSection title="Cluster status and autoscaler" startExpanded>
        {
          <OverviewCardsContainer>
            <StyledNodeCountCard />
            <StyledOverviewCard>
              <NodeStatusCard clusterStatus={clusterStatus} />
            </StyledOverviewCard>
            <StyledOverviewCard>
              <ResourceStatusCard clusterStatus={clusterStatus} />
            </StyledOverviewCard>
          </OverviewCardsContainer>
        }
      </StyledCollapsibleSection>

      <StyledCollapsibleSection title="Events" startExpanded>
        <EventTable />
      </StyledCollapsibleSection>
    </RootDiv>
  );
};
