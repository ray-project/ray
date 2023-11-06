import { createStyles, makeStyles, Typography } from "@material-ui/core";
import classNames from "classnames";
import React, { useContext } from "react";
import { GlobalContext } from "../../../App";
import { GrafanaNotRunningAlert } from "../../metrics";
import { LinkWithArrow, OverviewCard } from "./OverviewCard";

const useStyles = makeStyles((theme) =>
  createStyles({
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
  }),
);

type NodeCountCardProps = {
  className?: string;
};

export const NodeCountCard = ({ className }: NodeCountCardProps) => {
  const classes = useStyles();

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
    <OverviewCard className={classNames(classes.root, className)}>
      {grafanaHost === undefined || !prometheusHealth ? (
        <div className={classes.noGraph}>
          <Typography variant="h3">Node count</Typography>
          <GrafanaNotRunningAlert className={classes.alert} severity="info" />
        </div>
      ) : (
        <iframe
          title="Node Count"
          className={classes.graph}
          src={`${grafanaHost}${path}&refresh${timeRangeParams}&var-SessionName=${sessionName}`}
          frameBorder="0"
        />
      )}
      <div className={classes.links}>
        <LinkWithArrow text="View all nodes" to="/cluster" />
      </div>
    </OverviewCard>
  );
};
