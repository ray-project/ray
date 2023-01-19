import { createStyles, makeStyles, Typography } from "@material-ui/core";
import classNames from "classnames";
import React, { useContext } from "react";
import { Link, Outlet } from "react-router-dom";
import Logo from "../../logo.svg";
import { MainNavContext, useMainNavState } from "./mainNavContext";

const MAIN_NAV_HEIGHT = 56;
const BREADCRUMBS_HEIGHT = 36;

const useStyles = makeStyles((theme) =>
  createStyles({
    nav: {
      position: "fixed",
      width: "100%",
      backgroundColor: "white",
      zIndex: 10000,
    },
  }),
);

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
  const classes = useStyles();
  const mainNavContextState = useMainNavState();

  return (
    <MainNavContext.Provider value={mainNavContextState}>
      <nav className={classes.nav}>
        <MainNavBar />
        <MainNavBreadcrumbs />
      </nav>
      <Main />
    </MainNavContext.Provider>
  );
};

const useMainStyles = makeStyles((theme) =>
  createStyles({
    root: {
      paddingTop: MAIN_NAV_HEIGHT,
    },
    withTallNav: {
      // When breadcrumbs are also shown
      paddingTop: MAIN_NAV_HEIGHT + BREADCRUMBS_HEIGHT + 2, // +2 for border
    },
  }),
);

const Main = () => {
  const classes = useMainStyles();
  const { mainNavPageHierarchy } = useContext(MainNavContext);

  const tallNav = mainNavPageHierarchy.length > 1;

  return (
    <main
      className={classNames(classes.root, { [classes.withTallNav]: tallNav })}
    >
      <Outlet />
    </main>
  );
};

const useMainNavBarStyles = makeStyles((theme) =>
  createStyles({
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
    navItem: {
      marginRight: theme.spacing(6),
      fontSize: "1rem",
      fontWeight: 500,
      color: "black",
      textDecoration: "none",
    },
    navItemHighlighted: {
      color: "#036DCF",
    },
  }),
);

const NAV_ITEMS = [
  {
    title: "Overview",
    path: "/new/overview",
    id: "overview",
  },
  {
    title: "Jobs",
    path: "/new/jobs",
    id: "jobs",
  },
  {
    title: "Cluster",
    path: "/new/cluster",
    id: "cluster",
  },
  {
    title: "Logs",
    path: "/new/logs",
    id: "logs",
  },
];

const MainNavBar = () => {
  const classes = useMainNavBarStyles();
  const { mainNavPageHierarchy } = useContext(MainNavContext);
  const rootRouteId = mainNavPageHierarchy[0]?.id;

  return (
    <div className={classes.root}>
      <Link className={classes.logo} to="/new">
        <img width={28} src={Logo} alt="Ray" />
      </Link>
      {/* TODO (aguo): Get rid of /new prefix */}
      {NAV_ITEMS.map(({ title, path, id }) => (
        <Typography key={id}>
          <Link
            className={classNames(classes.navItem, {
              [classes.navItemHighlighted]: rootRouteId === id,
            })}
            to={path}
          >
            {title}
          </Link>
        </Typography>
      ))}
    </div>
  );
};

const useMainNavBreadcrumbsStyles = makeStyles((theme) =>
  createStyles({
    root: {
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
    },
    breadcrumbItem: {
      fontWeight: 500,
      "&:not(:first-child)": {
        marginLeft: theme.spacing(1),
      },
    },
    link: {
      textDecoration: "none",
      color: "#8C9196",
    },
    currentItem: {
      color: "black",
    },
  }),
);

const MainNavBreadcrumbs = () => {
  const classes = useMainNavBreadcrumbsStyles();
  const { mainNavPageHierarchy } = useContext(MainNavContext);

  if (mainNavPageHierarchy.length <= 1) {
    // Only render breadcrumbs if we have at least 2 items
    return null;
  }

  return (
    <div className={classes.root}>
      {mainNavPageHierarchy.map(({ title, id, path }, index) => {
        const linkOrText = path ? (
          <Link
            className={classNames(classes.link, {
              [classes.currentItem]: index === mainNavPageHierarchy.length - 1,
            })}
            to={path}
          >
            {title}
          </Link>
        ) : (
          title
        );
        if (index === 0) {
          return (
            <Typography key={id} className={classes.breadcrumbItem}>
              {linkOrText}
            </Typography>
          );
        } else {
          return (
            <React.Fragment key={id}>
              <Typography className={classes.breadcrumbItem}>{"/"}</Typography>
              <Typography className={classes.breadcrumbItem}>
                {linkOrText}
              </Typography>
            </React.Fragment>
          );
        }
      })}
    </div>
  );
};
