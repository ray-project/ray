import {
  Box,
  IconButton,
  Link,
  Theme,
  Tooltip,
  Typography,
  useTheme,
} from "@mui/material";
import React, { useContext } from "react";
import { RiBookMarkLine, RiFeedbackLine } from "react-icons/ri/";
import { Outlet, Link as RouterLink } from "react-router-dom";
import { GlobalContext } from "../../App";
import Logo from "../../logo.svg";
import { MainNavContext, useMainNavState } from "./mainNavContext";

export const MAIN_NAV_HEIGHT = 56;
export const BREADCRUMBS_HEIGHT = 36;

const useStyles = (theme: Theme) => ({
  nav: {
    position: "fixed",
    width: "100%",
    backgroundColor: "white",
    zIndex: 1000,
  },
});

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
  const styles = useStyles(useTheme());
  const mainNavContextState = useMainNavState();

  return (
    <MainNavContext.Provider value={mainNavContextState}>
      <Box component="nav" sx={styles.nav}>
        <MainNavBar />
        <MainNavBreadcrumbs />
      </Box>
      <Main />
    </MainNavContext.Provider>
  );
};

const useMainStyles = (theme: Theme) => ({
  root: (tallNav: boolean) => ({
    paddingTop: tallNav
      ? `${MAIN_NAV_HEIGHT + BREADCRUMBS_HEIGHT + 2}px` //When breadcrumbs are also shown, +2 for border
      : `${MAIN_NAV_HEIGHT}px`,
  }),
});

const Main = () => {
  const styles = useMainStyles(useTheme());
  const { mainNavPageHierarchy } = useContext(MainNavContext);

  const tallNav = mainNavPageHierarchy.length > 1;

  return (
    <Box component="main" sx={styles.root(tallNav)}>
      <Outlet />
    </Box>
  );
};

const useMainNavBarStyles = (theme: Theme) => ({
  root: {
    display: "flex",
    flexDirection: "row",
    flexWrap: "nowrap",
    height: 56,
    backgroundColor: "white",
    alignItems: "center",
    boxShadow: "0px 1px 0px #D2DCE6",
  },
  logo: {
    display: "flex",
    justifyContent: "center",
    marginLeft: theme.spacing(2),
    marginRight: theme.spacing(3),
  },
  navItem: (highlight: boolean) => ({
    marginRight: theme.spacing(6),
    fontSize: "1rem",
    fontWeight: 500,
    color: highlight ? "#036DCF" : "black",
    textDecoration: "none",
  }),
  flexSpacer: {
    flexGrow: 1,
  },
  actionItemsContainer: {
    marginRight: theme.spacing(2),
  },
  backToOld: {
    marginRight: theme.spacing(1.5),
    textDecoration: "none",
  },
  backToOldText: {
    letterSpacing: 0.25,
    fontWeight: 500,
  },
  actionItem: {
    color: "#5F6469",
  },
});

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
  const styles = useMainNavBarStyles(useTheme());
  const { mainNavPageHierarchy } = useContext(MainNavContext);
  const rootRouteId = mainNavPageHierarchy[0]?.id;
  const { metricsContextLoaded, grafanaHost } = useContext(GlobalContext);

  let navItems = NAV_ITEMS;
  if (!metricsContextLoaded || grafanaHost === "DISABLED") {
    navItems = navItems.filter(({ id }) => id !== "metrics");
  }

  return (
    <Box sx={styles.root}>
      <Link component={RouterLink} sx={styles.logo} to="/">
        <img width={28} src={Logo} alt="Ray" />
      </Link>
      {navItems.map(({ title, path, id }) => (
        <Typography key={id}>
          <Link
            component={RouterLink}
            sx={styles.navItem(rootRouteId === id)}
            to={path}
          >
            {title}
          </Link>
        </Typography>
      ))}
      <Box sx={styles.flexSpacer}></Box>
      <Box sx={styles.actionItemsContainer}>
        <Tooltip title="Docs">
          <IconButton
            sx={styles.actionItem}
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
            sx={styles.actionItem}
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

const useMainNavBreadcrumbsStyles = (theme: Theme) => ({
  root: {
    display: "flex",
    flexDirection: "row",
    flexWrap: "nowrap",
    height: 36,
    marginTop: "1px",
    paddingLeft: theme.spacing(2),
    paddingRight: theme.spacing(2),
    backgroundColor: "white",
    alignItems: "center",
    boxShadow: "0px 1px 0px #D2DCE6",
  },
  breadcrumbItem: {
    fontWeight: 500,
    color: "#8C9196",
    "&:not(:first-child)": {
      marginLeft: theme.spacing(1),
    },
  },
  link: (currentItem: boolean) => ({
    textDecoration: "none",
    color: currentItem ? "black" : "#8C9196",
  }),
});

const MainNavBreadcrumbs = () => {
  const styles = useMainNavBreadcrumbsStyles(useTheme());
  const { mainNavPageHierarchy } = useContext(MainNavContext);

  if (mainNavPageHierarchy.length <= 1) {
    // Only render breadcrumbs if we have at least 2 items
    return null;
  }

  let currentPath = "";

  return (
    <Box sx={styles.root}>
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
            sx={styles.link(index === mainNavPageHierarchy.length - 1)}
            to={currentPath}
          >
            {title}
          </Link>
        ) : (
          title
        );
        if (index === 0) {
          return (
            <Typography key={id} sx={styles.breadcrumbItem} variant="body2">
              {linkOrText}
            </Typography>
          );
        } else {
          return (
            <React.Fragment key={id}>
              <Typography sx={styles.breadcrumbItem} variant="body2">
                {"/"}
              </Typography>
              <Typography sx={styles.breadcrumbItem} variant="body2">
                {linkOrText}
              </Typography>
            </React.Fragment>
          );
        }
      })}
    </Box>
  );
};
