import { createStyles, makeStyles } from "@material-ui/core";
import React from "react";
import { Outlet } from "react-router-dom";
import { MainNavPageInfo } from "../layout/mainNavContext";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(3),
      width: "100%",
      minHeight: 800,
      background: "white",
    },
  }),
);

export const ServeLayout = () => {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={{
          id: "serve",
          title: "Serve",
          path: "/serve",
        }}
      />
      <Outlet />
    </div>
  );
};
