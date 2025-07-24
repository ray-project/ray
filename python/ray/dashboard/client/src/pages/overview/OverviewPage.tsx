import { Box, Theme } from "@mui/material";
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

const styles = {
  overviewCard: (theme: Theme) => ({
    flex: "1 0 448px",
    maxWidth: "100%",
    [theme.breakpoints.up("md")]: {
      // Calculate max width based on 1/3 of the total width minus padding between cards
      maxWidth: `calc((100% - ${theme.spacing(3)} * 2) / 3)`,
    },
  }),
  autoscalerCard: {
    backgroundColor: "white",
    paddingX: 3,
    paddingY: 2,
  },
};

export const OverviewPage = () => {
  const { clusterStatus } = useRayStatus();

  return (
    <Box sx={{ padding: 3, backgroundColor: "white" }}>
      <MainNavPageInfo
        pageInfo={{ title: "Overview", id: "overview", path: "/overview" }}
      />
      <Box
        sx={(theme: Theme) => ({
          display: "flex",
          flexDirection: "row",
          flexWrap: "wrap",
          marginBottom: 4,
          gap: 3,
          [theme.breakpoints.up("md")]: {
            flexWrap: "nowrap",
          },
        })}
      >
        <ClusterUtilizationCard sx={styles.overviewCard} />
        <RecentJobsCard sx={styles.overviewCard} />
        <RecentServeCard sx={styles.overviewCard} />
      </Box>

      <CollapsibleSection
        sx={{ marginTop: 4 }}
        title="Cluster status and autoscaler"
        startExpanded
      >
        {
          <Box
            sx={(theme: Theme) => ({
              display: "flex",
              flexDirection: "row",
              flexWrap: "wrap",
              marginBottom: 4,
              gap: 3,
              [theme.breakpoints.up("md")]: {
                flexWrap: "nowrap",
              },
            })}
          >
            <NodeCountCard sx={styles.overviewCard} />
            <OverviewCard sx={[styles.overviewCard, styles.autoscalerCard]}>
              <NodeStatusCard clusterStatus={clusterStatus} />
            </OverviewCard>
            <OverviewCard sx={[styles.overviewCard, styles.autoscalerCard]}>
              <ResourceStatusCard clusterStatus={clusterStatus} />
            </OverviewCard>
          </Box>
        }
      </CollapsibleSection>

      <CollapsibleSection sx={{ marginTop: 4 }} title="Events" startExpanded>
        <EventTable />
      </CollapsibleSection>
    </Box>
  );
};
