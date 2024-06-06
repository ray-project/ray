import { IconButton, Tooltip, Typography } from "@mui/material";
import { styled } from "@mui/material/styles";
import React, { useContext } from "react";
import { RiBookMarkLine, RiFeedbackLine } from "react-icons/ri/";
import { Link, Outlet } from "react-router-dom";
import { GlobalContext } from "../../App";
import Logo from "../../logo.svg";
import { MainNavContext, useMainNavState } from "./mainNavContext";

export const MAIN_NAV_HEIGHT = 56;
export const BREADCRUMBS_HEIGHT = 36;

const StyledNav = styled("nav")(({ theme }) => ({
  position: "fixed",
  width: "100%",
  backgroundColor: "white",
  zIndex: 1000,
}));

/**
 * This is the main navigation stack of the entire application.
 * Only certain pages belong to the main navigation stack.
 *
 * This layout is always shown at the top with at least one row and up to two rows.
 * - The first row shows all the top level pages of the main navigation stack and
 *   highlights the top level page that the user is currently on.
 * - The second row show breadcrumbs of the main navigation stack.
 *   If we are at a top level page (i.e. the breadcrumbs is of length 1),
 *   we do not show the breadcrumbs.
 *
 * To use this layout, simply create use react-router-6 nested routes to produce the
 * correct hierarchy. Then for the routes which should be considered part of the main
 * navigation stack, render the <MainNavPageInfo /> component at the top of the route.
 */
export const MainNavLayout = () => {
  const mainNavContextState = useMainNavState();

  return (
    <MainNavContext.Provider value={mainNavContextState}>
      <StyledNav>
        <MainNavBar />
        <MainNavBreadcrumbs />
      </StyledNav>
      <Main />
    </MainNavContext.Provider>
  );
};

const RootMain = styled("main")<{ tallNav?: boolean }>(
  ({ theme, tallNav }) => ({
    paddingTop: tallNav
      ? MAIN_NAV_HEIGHT + BREADCRUMBS_HEIGHT + 2
      : MAIN_NAV_HEIGHT,
  }),
);

const Main = () => {
  const { mainNavPageHierarchy } = useContext(MainNavContext);

  const tallNav = mainNavPageHierarchy.length > 1;

  return (
    <RootMain tallNav={tallNav}>
      <Outlet />
    </RootMain>
  );
};

const RootDiv = styled("div")(({ theme }) => ({
  display: "flex",
  flexDirection: "row",
  flexWrap: "nowrap",
  height: 56,
  backgroundColor: "white",
  alignItems: "center",
  boxShadow: "0px 1px 0px #D2DCE6",
}));

const LogoLink = styled(Link)(({ theme }) => ({
  display: "flex",
  justifyContent: "center",
  marginLeft: theme.spacing(2),
  marginRight: theme.spacing(3),
}));

const NavItemLink = styled(Link)(({ theme }) => ({
  marginRight: theme.spacing(6),
  fontSize: "1rem",
  fontWeight: 500,
  color: "black",
  textDecoration: "none",
}));

const FlexSpacerDiv = styled("div")(({ theme }) => ({
  flexGrow: 1,
}));

const ActionItemsContainerDiv = styled("div")(({ theme }) => ({
  marginRight: theme.spacing(2),
}));

const ActionItemIconButton = styled(IconButton)(({ theme }) => ({
  color: "#5F6469",
})) as typeof IconButton;

const NAV_ITEMS = [
  {
    title: "Overview",
    path: "/overview",
    id: "overview",
  },
  {
    title: "Jobs",
    path: "/jobs",
    id: "jobs",
  },
  {
    title: "Serve",
    path: "/serve",
    id: "serve",
  },
  {
    title: "Cluster",
    path: "/cluster",
    id: "cluster",
  },
  {
    title: "Actors",
    path: "/actors",
    id: "actors",
  },
  {
    title: "Metrics",
    path: "/metrics",
    id: "metrics",
  },
  {
    title: "Logs",
    path: "/logs",
    id: "logs",
  },
];

const MainNavBar = () => {
  const { mainNavPageHierarchy } = useContext(MainNavContext);
  const rootRouteId = mainNavPageHierarchy[0]?.id;
  const { metricsContextLoaded, grafanaHost } = useContext(GlobalContext);

  let navItems = NAV_ITEMS;
  if (!metricsContextLoaded || grafanaHost === "DISABLED") {
    navItems = navItems.filter(({ id }) => id !== "metrics");
  }

  return (
    <RootDiv>
      <LogoLink to="/">
        <img width={28} src={Logo} alt="Ray" />
      </LogoLink>
      {navItems.map(({ title, path, id }) => (
        <Typography key={id}>
          <NavItemLink
            sx={[rootRouteId === id && { color: "#036DCF" }]}
            to={path}
          >
            {title}
          </NavItemLink>
        </Typography>
      ))}
      <FlexSpacerDiv />
      <ActionItemsContainerDiv>
        <Tooltip title="Docs">
          <ActionItemIconButton
            href="https://docs.ray.io/en/latest/ray-core/ray-dashboard.html"
            target="_blank"
            rel="noopener noreferrer"
            size="large"
          >
            <RiBookMarkLine />
          </ActionItemIconButton>
        </Tooltip>
        <Tooltip title="Leave feedback">
          <ActionItemIconButton
            href="https://github.com/ray-project/ray/issues/new?assignees=&labels=bug%2Ctriage%2Cdashboard&template=bug-report.yml&title=%5BDashboard%5D+%3CTitle%3E"
            target="_blank"
            rel="noopener noreferrer"
            size="large"
          >
            <RiFeedbackLine />
          </ActionItemIconButton>
        </Tooltip>
      </ActionItemsContainerDiv>
    </RootDiv>
  );
};

const MainNavBreadcrumbsRoot = styled("div")(({ theme }) => ({
  display: "flex",
  flexDirection: "row",
  flexWrap: "nowrap",
  height: 36,
  marginTop: 1,
  paddingLeft: theme.spacing(2),
  paddingRight: theme.spacing(2),
  backgroundColor: "white",
  alignItems: "center",
  boxShadow: "0px 1px 0px #D2DCE6",
}));

const BreadcrumbItemTypography = styled(Typography)(({ theme }) => ({
  fontWeight: 500,
  color: "#8C9196",
  "&:not(:first-of-type)": {
    marginLeft: theme.spacing(1),
  },
}));

const StyledLink = styled(Link)(({ theme }) => ({
  textDecoration: "none",
  color: "#8C9196",
}));

const MainNavBreadcrumbs = () => {
  const { mainNavPageHierarchy } = useContext(MainNavContext);

  if (mainNavPageHierarchy.length <= 1) {
    // Only render breadcrumbs if we have at least 2 items
    return null;
  }

  let currentPath = "";

  return (
    <MainNavBreadcrumbsRoot>
      {mainNavPageHierarchy.map(({ title, id, path }, index) => {
        if (path) {
          if (path.startsWith("/")) {
            currentPath = path;
          } else {
            currentPath = `${currentPath}/${path}`;
          }
        }
        const linkOrText = path ? (
          <StyledLink
            sx={[
              index === mainNavPageHierarchy.length - 1 && { color: "black" },
            ]}
            to={currentPath}
          >
            {title}
          </StyledLink>
        ) : (
          title
        );
        if (index === 0) {
          return (
            <BreadcrumbItemTypography key={id} variant="body2">
              {linkOrText}
            </BreadcrumbItemTypography>
          );
        } else {
          return (
            <React.Fragment key={id}>
              <BreadcrumbItemTypography variant="body2">
                {"/"}
              </BreadcrumbItemTypography>
              <BreadcrumbItemTypography variant="body2">
                {linkOrText}
              </BreadcrumbItemTypography>
            </React.Fragment>
          );
        }
      })}
    </MainNavBreadcrumbsRoot>
  );
};
