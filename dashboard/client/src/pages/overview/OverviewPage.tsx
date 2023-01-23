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
    section: {
      marginTop: theme.spacing(4),
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
        <ClusterUtilizationCard className={classes.overviewCard} />
        <NodeCountCard className={classes.overviewCard} />
        <RecentJobsCard className={classes.overviewCard} />
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
        keepRendered
      >
        <Metrics />
      </CollapsibleSection>
    </div>
  );
};
