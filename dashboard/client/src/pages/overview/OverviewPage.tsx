import { createStyles, makeStyles, Typography } from "@material-ui/core";
import React from "react";
import EventTable from "../../components/EventTable";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { Metrics } from "../metrics";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(3),
      backgroundColor: "white",
    },
    metricsContainer: {
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
      <div>
        <Typography variant="h1">Events</Typography>
        <EventTable />
      </div>
      <div className={classes.metricsContainer}>
        <Typography variant="h1">Node metrics</Typography>
        <Metrics />
      </div>
    </div>
  );
};
