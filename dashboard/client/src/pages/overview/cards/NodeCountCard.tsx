import { createStyles, makeStyles, Typography } from "@material-ui/core";
import classNames from "classnames";
import React, { useContext } from "react";
import { Link } from "react-router-dom";
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
    links: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
    },
    link: {
      color: "#036DCF",
      textDecoration: "none",
    },
    nodesLink: {
      marginRight: theme.spacing(3),
    },
  }),
);

export const NodeCountCard = () => {
  const classes = useStyles();

  const { grafanaHost, sessionName } = useContext(GlobalContext);
  const path =
    "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=24";
  const timeRangeParams = "&from=now-30m&to=now";

  return (
    <OverviewCard className={classes.root}>
      {/* TODO (aguo): Handle grafana not running */}
      <iframe
        title="Node Count"
        className={classes.graph}
        src={`${grafanaHost}${path}&refresh${timeRangeParams}&var-SessionName=${sessionName}`}
        width="416"
        frameBorder="0"
      />
      <div className={classes.links}>
        <Link
          className={classNames(classes.link, classes.nodesLink)}
          to="/new/cluster"
        >
          <Typography variant="h4">View all nodes →</Typography>
        </Link>
      </div>
    </OverviewCard>
  );
};
