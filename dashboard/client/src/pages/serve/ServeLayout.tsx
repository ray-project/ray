import createStyles from "@mui/styles/createStyles";
import makeStyles from "@mui/styles/makeStyles";
import React from "react";
import { RiInformationLine, RiTableLine } from "react-icons/ri";
import { Outlet } from "react-router-dom";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { SideTabLayout, SideTabRouteLink } from "../layout/SideTabLayout";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
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

export const ServeSideTabLayout = () => {
  return (
    <SideTabLayout>
      <SideTabRouteLink
        tabId="system"
        title="System"
        Icon={RiInformationLine}
      />
      <SideTabRouteLink
        to=""
        tabId="applications"
        title="Applications"
        Icon={RiTableLine}
      />
    </SideTabLayout>
  );
};
