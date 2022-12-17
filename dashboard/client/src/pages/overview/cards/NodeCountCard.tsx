import { createStyles, makeStyles } from "@material-ui/core";
import classNames from "classnames";
import React, { useContext } from "react";
import { GlobalContext } from "../../../App";
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
    links: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
    },
  }),
);

type NodeCountCardProps = {
  className?: string;
};

export const NodeCountCard = ({ className }: NodeCountCardProps) => {
  const classes = useStyles();

  const { grafanaHost, sessionName } = useContext(GlobalContext);
  const path =
    "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=24";
  const timeRangeParams = "&from=now-30m&to=now";

  return (
    <OverviewCard className={classNames(classes.root, className)}>
      {/* TODO (aguo): Handle grafana not running */}
      <iframe
        title="Node Count"
        className={classes.graph}
        src={`${grafanaHost}${path}&refresh${timeRangeParams}&var-SessionName=${sessionName}`}
        width="416"
        frameBorder="0"
      />
      <div className={classes.links}>
        <LinkWithArrow text="View all nodes" to="/new/cluster" />
      </div>
    </OverviewCard>
  );
};
