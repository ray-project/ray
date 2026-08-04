import { Box, useTheme } from "@mui/material";
import React from "react";
import { RiInformationLine, RiTableLine } from "react-icons/ri";
import { Outlet } from "react-router-dom";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { SideTabLayout, SideTabRouteLink } from "../layout/SideTabLayout";

export const ServeLayout = () => {
  const theme = useTheme();
  return (
    <Box
      sx={{
        width: "100%",
        minHeight: 800,
        background: theme.palette.background.default,
      }}
    >
      <MainNavPageInfo
        pageInfo={{
          id: "serve",
          title: "Serve",
          path: "/serve",
        }}
      />
      <Outlet />
    </Box>
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
