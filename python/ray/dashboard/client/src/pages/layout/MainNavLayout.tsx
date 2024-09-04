import { Box, IconButton, Link, Tooltip, Typography } from "@mui/material";
import React, { useContext } from "react";
import { RiBookMarkLine, RiFeedbackLine } from "react-icons/ri/";
import { Outlet, Link as RouterLink } from "react-router-dom";
import { GlobalContext } from "../../App";
import Logo from "../../logo.svg";
import { MainNavContext, useMainNavState } from "./mainNavContext";

export const MAIN_NAV_HEIGHT = 56;
export const BREADCRUMBS_HEIGHT = 36;

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
      <Box
        component="nav"
        sx={{
          position: "fixed",
          width: "100%",
          backgroundColor: "white",
          zIndex: 1000,
        }}
      >
        <MainNavBar />
        <MainNavBreadcrumbs />
      </Box>
      <Main />
    </MainNavContext.Provider>
  );
};

const Main = () => {
  const { mainNavPageHierarchy } = useContext(MainNavContext);

  const tallNav = mainNavPageHierarchy.length > 1;

  return (
    <Box
      component="main"
      sx={{
        paddingTop: tallNav
          ? `${MAIN_NAV_HEIGHT + BREADCRUMBS_HEIGHT + 2}px` //When breadcrumbs are also shown, +2 for border
          : `${MAIN_NAV_HEIGHT}px`,
      }}
    >
      <Outlet />
    </Box>
  );
};

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
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        flexWrap: "nowrap",
        height: 56,
        backgroundColor: "white",
        alignItems: "center",
        boxShadow: "0px 1px 0px #D2DCE6",
      }}
    >
      <Link
        component={RouterLink}
        sx={{
          display: "flex",
          justifyContent: "center",
          marginLeft: 2,
          marginRight: 3,
        }}
        to="/"
      >
        <img width={28} src={Logo} alt="Ray" />
      </Link>
      {navItems.map(({ title, path, id }) => (
        <Typography key={id}>
          <Link
            component={RouterLink}
            sx={{
              marginRight: 6,
              fontSize: "1rem",
              fontWeight: 500,
              color: rootRouteId === id ? "#036DCF" : "black",
              textDecoration: "none",
            }}
            to={path}
          >
            {title}
          </Link>
        </Typography>
      ))}
      <Box sx={{ flexGrow: 1 }}></Box>
      <Box sx={{ marginRight: 2 }}>
        <Tooltip title="Docs">
          <IconButton
            sx={{ color: "#5F6469" }}
            href="https://docs.ray.io/en/latest/ray-core/ray-dashboard.html"
            target="_blank"
            rel="noopener noreferrer"
            size="large"
          >
            <RiBookMarkLine />
          </IconButton>
        </Tooltip>
        <Tooltip title="Leave feedback">
          <IconButton
            sx={{ color: "#5F6469" }}
            href="https://github.com/ray-project/ray/issues/new?assignees=&labels=bug%2Ctriage%2Cdashboard&template=bug-report.yml&title=%5BDashboard%5D+%3CTitle%3E"
            target="_blank"
            rel="noopener noreferrer"
            size="large"
          >
            <RiFeedbackLine />
          </IconButton>
        </Tooltip>
      </Box>
    </Box>
  );
};

const MainNavBreadcrumbs = () => {
  const { mainNavPageHierarchy } = useContext(MainNavContext);

  if (mainNavPageHierarchy.length <= 1) {
    // Only render breadcrumbs if we have at least 2 items
    return null;
  }

  let currentPath = "";

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        flexWrap: "nowrap",
        height: 36,
        marginTop: "1px",
        paddingLeft: 2,
        paddingRight: 2,
        backgroundColor: "white",
        alignItems: "center",
        boxShadow: "0px 1px 0px #D2DCE6",
      }}
    >
      {mainNavPageHierarchy.map(({ title, id, path }, index) => {
        if (path) {
          if (path.startsWith("/")) {
            currentPath = path;
          } else {
            currentPath = `${currentPath}/${path}`;
          }
        }
        const linkOrText = path ? (
          <Link
            component={RouterLink}
            sx={{
              textDecoration: "none",
              color:
                index === mainNavPageHierarchy.length - 1 ? "black" : "#8C9196",
            }}
            to={currentPath}
          >
            {title}
          </Link>
        ) : (
          title
        );
        if (index === 0) {
          return (
            <Typography
              key={id}
              sx={{
                fontWeight: 500,
                color: "#8C9196",
                "&:not(:first-child)": {
                  marginLeft: 1,
                },
              }}
              variant="body2"
            >
              {linkOrText}
            </Typography>
          );
        } else {
          return (
            <React.Fragment key={id}>
              <Typography
                sx={{
                  fontWeight: 500,
                  color: "#8C9196",
                  "&:not(:first-child)": {
                    marginLeft: 1,
                  },
                }}
                variant="body2"
              >
                {"/"}
              </Typography>
              <Typography
                sx={{
                  fontWeight: 500,
                  color: "#8C9196",
                  "&:not(:first-child)": {
                    marginLeft: 1,
                  },
                }}
                variant="body2"
              >
                {linkOrText}
              </Typography>
            </React.Fragment>
          );
        }
      })}
    </Box>
  );
};
