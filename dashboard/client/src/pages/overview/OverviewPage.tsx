import { createStyles, makeStyles } from "@material-ui/core";
import React from "react";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import EventTable from "../../components/EventTable";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { Metrics } from "../metrics";
import { ClusterUtilizationCard } from "./cards/ClusterUtilizationCard";
import { NodeCountCard } from "./cards/NodeCountCard";
import { RecentJobsCard } from "./cards/RecentJobsCard";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(3),
      backgroundColor: "white",
    },
    overviewCardsContainer: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
      marginBottom: theme.spacing(4),
      gap: theme.spacing(3),
    },
    section: {
      marginTop: theme.spacing(2),
    },
  }),
);

export const OverviewPage = () => {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={{ title: "Overview", id: "overview", path: "/new/overview" }}
      />
      <div className={classes.overviewCardsContainer}>
        <ClusterUtilizationCard />
        <NodeCountCard />
        <RecentJobsCard />
      </div>

      <CollapsibleSection
        className={classes.section}
        title="Events"
        startExpanded
      >
        <EventTable />
      </CollapsibleSection>

      {/* TODO (aguo): Make section match the design */}
      <CollapsibleSection
        className={classes.section}
        title="Node metrics"
        startExpanded
      >
        <Metrics />
      </CollapsibleSection>
    </div>
  );
};
