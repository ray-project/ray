import { createStyles, makeStyles } from "@material-ui/core";
import classNames from "classnames";
import React from "react";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import {
  NodeStatusCard,
  ResourceStatusCard,
} from "../../components/AutoscalerStatusCards";
import NewEventTable from "../../components/NewEventTable";
import { SeverityLevel } from "../../type/event";
import { useRayStatus } from "../job/hook/useClusterStatus";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { ClusterUtilizationCard } from "./cards/ClusterUtilizationCard";
import { NodeCountCard } from "./cards/NodeCountCard";
import { OverviewCard } from "./cards/OverviewCard";
import { RecentJobsCard } from "./cards/RecentJobsCard";
import { RecentServeCard } from "./cards/RecentServeCard";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(3),
      backgroundColor: "white",
    },
    overviewCardsContainer: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "wrap",
      marginBottom: theme.spacing(4),
      gap: theme.spacing(3),
      [theme.breakpoints.up("md")]: {
        flexWrap: "nowrap",
      },
    },
    overviewCard: {
      flex: "1 0 448px",
      maxWidth: "100%",
      [theme.breakpoints.up("md")]: {
        // Calculate max width based on 1/3 of the total width minus padding between cards
        maxWidth: `calc((100% - ${theme.spacing(3)}px * 2) / 3)`,
      },
    },
    autoscalerCard: {
      padding: theme.spacing(2, 3),
    },
    section: {
      marginTop: theme.spacing(4),
    },
  }),
);

export const OverviewPage = () => {
  const classes = useStyles();

  const { cluster_status } = useRayStatus();

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={{ title: "Overview", id: "overview", path: "/overview" }}
      />
      <div className={classes.overviewCardsContainer}>
        <ClusterUtilizationCard className={classes.overviewCard} />
        <RecentJobsCard className={classes.overviewCard} />
        <RecentServeCard className={classes.overviewCard} />
      </div>

      <CollapsibleSection
        className={classes.section}
        title="Cluster status and autoscaler"
        startExpanded
      >
        {
          <div className={classes.overviewCardsContainer}>
            <NodeCountCard className={classes.overviewCard} />
            <OverviewCard
              className={classNames(
                classes.root,
                classes.overviewCard,
                classes.autoscalerCard,
              )}
            >
              <NodeStatusCard cluster_status={cluster_status} />
            </OverviewCard>
            <OverviewCard
              className={classNames(
                classes.root,
                classes.overviewCard,
                classes.autoscalerCard,
              )}
            >
              <ResourceStatusCard cluster_status={cluster_status} />
            </OverviewCard>
          </div>
        }
      </CollapsibleSection>

      <CollapsibleSection
        className={classes.section}
        title="Events"
        startExpanded
      >
        <NewEventTable
          defaultSeverityLevels={[SeverityLevel.WARNING, SeverityLevel.ERROR]}
        />
      </CollapsibleSection>
    </div>
  );
};
