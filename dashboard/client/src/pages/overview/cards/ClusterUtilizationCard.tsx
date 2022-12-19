import { createStyles, makeStyles } from "@material-ui/core";
import classNames from "classnames";
import React, { useContext } from "react";
import { GlobalContext } from "../../../App";
import { OverviewCard } from "./OverviewCard";

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
  }),
);

type ClusterUtilizationCardProps = {
  className?: string;
};

export const ClusterUtilizationCard = ({
  className,
}: ClusterUtilizationCardProps) => {
  const classes = useStyles();

  const { grafanaHost, sessionName } = useContext(GlobalContext);
  const path =
    "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=2";
  const timeRangeParams = "&from=now-30m&to=now";

  return (
    <OverviewCard className={classNames(classes.root, className)}>
      {/* TODO (aguo): Switch this to overall utilization graph */}
      {/* TODO (aguo): Handle grafana not running */}
      <iframe
        title="Cluster Utilization"
        className={classes.graph}
        src={`${grafanaHost}${path}&refresh${timeRangeParams}&var-SessionName=${sessionName}`}
        frameBorder="0"
      />
    </OverviewCard>
  );
};
