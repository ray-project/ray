import { styled } from "@mui/material/styles";
import React from "react";
import { RiInformationLine, RiTableLine } from "react-icons/ri";
import { Outlet } from "react-router-dom";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { SideTabLayout, SideTabRouteLink } from "../layout/SideTabLayout";

const RootDiv = styled("div")(({ theme }) => ({
  width: "100%",
  minHeight: 800,
  background: "white",
}));

export const ServeLayout = () => {
  return (
    <RootDiv>
      <MainNavPageInfo
        pageInfo={{
          id: "serve",
          title: "Serve",
          path: "/serve",
        }}
      />
      <Outlet />
    </RootDiv>
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
