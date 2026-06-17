import { Box, IconButton, Link, Tooltip, Typography } from "@mui/material";
import React, { useContext } from "react";
import {
  RiBookMarkLine,
  RiFeedbackLine,
  RiMoonLine,
  RiRefreshLine,
  RiSunLine,
} from "react-icons/ri/";
import { Outlet, Link as RouterLink } from "react-router-dom";
import { useSWRConfig } from "swr";
import { GlobalContext } from "../../App";
import { DASHBOARD_DATA_LOADED_EVENT } from "../../common/constants";
import { SearchTimezone } from "../../components/SearchComponent";
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
        sx={(theme) => ({
          position: "fixed",
          width: "100%",
          backgroundColor: theme.palette.background.paper,
          zIndex: 1000,
        })}
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
  {
    title: "Platform Events",
    path: "/platform-events",
    id: "platform-events",
  },
];

const MainNavBar = () => {
  const { mainNavPageHierarchy } = useContext(MainNavContext);
  const rootRouteId = mainNavPageHierarchy[0]?.id;
  const {
    metricsContextLoaded,
    grafanaHost,
    platformEventsEnabled,
    serverTimeZone,
    currentTimeZone,
    themeMode,
    toggleTheme,
  } = useContext(GlobalContext);

  const urlTheme = new URLSearchParams(window.location.search).get("theme");

  let navItems = NAV_ITEMS;
  if (!metricsContextLoaded || grafanaHost === "DISABLED") {
    navItems = navItems.filter(({ id }) => id !== "metrics");
  }
  if (!platformEventsEnabled) {
    navItems = navItems.filter(({ id }) => id !== "platform-events");
  }

  return (
    <Box
      sx={(theme) => ({
        display: "flex",
        flexDirection: "row",
        flexWrap: "nowrap",
        height: 56,
        backgroundColor: theme.palette.background.paper,
        alignItems: "center",
        borderBottom: `1px solid ${theme.palette.divider}`,
      })}
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
            sx={(theme) => ({
              marginRight: 6,
              fontSize: "1rem",
              fontWeight: 500,
              color:
                rootRouteId === id
                  ? theme.palette.primary.main
                  : theme.palette.text.primary,
              textDecoration: "none",
            })}
            to={path}
          >
            {title}
          </Link>
        </Typography>
      ))}
      <Box sx={{ flexGrow: 1 }}></Box>
      <DashboardDataFreshness currentTimeZone={currentTimeZone} />
      <Box sx={{ marginRight: 2 }}>
        {!urlTheme && (
          <Tooltip title={themeMode === "light" ? "Dark mode" : "Light mode"}>
            <IconButton
              onClick={toggleTheme}
              sx={(theme) => ({ color: theme.palette.text.secondary })}
              size="large"
            >
              {themeMode === "light" ? <RiMoonLine /> : <RiSunLine />}
            </IconButton>
          </Tooltip>
        )}
        <Tooltip title="Docs">
          <IconButton
            sx={(theme) => ({ color: theme.palette.text.secondary })}
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
            sx={(theme) => ({ color: theme.palette.text.secondary })}
            href="https://github.com/ray-project/ray/issues/new?assignees=&labels=bug%2Ctriage%2Cdashboard&template=bug-report.yml&title=%5BDashboard%5D+%3CTitle%3E"
            target="_blank"
            rel="noopener noreferrer"
            size="large"
          >
            <RiFeedbackLine />
          </IconButton>
        </Tooltip>
      </Box>
      <Tooltip
        placement="left-start"
        title="The timezone of logs are not impacted by this selection."
      >
        <Box sx={{ marginRight: 3 }}>
          <SearchTimezone
            currentTimeZone={currentTimeZone}
            serverTimeZone={serverTimeZone}
          />
        </Box>
      </Tooltip>
    </Box>
  );
};

const DashboardDataFreshness = ({
  currentTimeZone,
}: {
  currentTimeZone: string | undefined;
}) => {
  const { mutate } = useSWRConfig();
  const [lastDataLoadTime, setLastDataLoadTime] = React.useState<number>();
  const [now, setNow] = React.useState(Date.now());
  const [isRefreshingData, setIsRefreshingData] = React.useState(false);

  React.useEffect(() => {
    const handleDashboardDataLoaded = () => {
      setLastDataLoadTime(Date.now());
    };

    window.addEventListener(
      DASHBOARD_DATA_LOADED_EVENT,
      handleDashboardDataLoaded,
    );

    return () => {
      window.removeEventListener(
        DASHBOARD_DATA_LOADED_EVENT,
        handleDashboardDataLoaded,
      );
    };
  }, []);

  React.useEffect(() => {
    const interval = window.setInterval(() => {
      setNow(Date.now());
    }, 5000);

    return () => {
      window.clearInterval(interval);
    };
  }, []);

  const refreshDashboardData = async () => {
    setIsRefreshingData(true);
    try {
      await mutate(() => true);
    } finally {
      setIsRefreshingData(false);
    }
  };

  return (
    <Box
      sx={{
        alignItems: "center",
        display: "flex",
        flexShrink: 0,
        gap: 1,
        marginRight: 1,
      }}
    >
      <Tooltip
        title={
          lastDataLoadTime
            ? `Last successful data load: ${formatLastDataLoadTime(
                lastDataLoadTime,
                currentTimeZone,
              )}`
            : "No dashboard data has loaded yet."
        }
      >
        <Typography
          color="text.secondary"
          sx={{ whiteSpace: "nowrap" }}
          variant="caption"
        >
          {formatFreshnessLabel(lastDataLoadTime, now)}
        </Typography>
      </Tooltip>
      <Tooltip title="Refresh dashboard data">
        <span>
          <IconButton
            aria-label="Refresh dashboard data"
            disabled={isRefreshingData}
            onClick={refreshDashboardData}
            size="large"
            sx={(theme) => ({ color: theme.palette.text.secondary })}
          >
            <RiRefreshLine />
          </IconButton>
        </span>
      </Tooltip>
    </Box>
  );
};

export const formatLastDataLoadTime = (
  lastDataLoadTime: number,
  currentTimeZone: string | undefined,
) => {
  return new Date(lastDataLoadTime).toLocaleString(
    undefined,
    currentTimeZone ? { timeZone: currentTimeZone } : undefined,
  );
};

export const formatFreshnessLabel = (
  lastDataLoadTime: number | undefined,
  now: number,
) => {
  if (!lastDataLoadTime) {
    return "No data loaded";
  }

  const ageMs = Math.max(0, now - lastDataLoadTime);
  const fiveSecondsMs = 5 * 1000;
  const minuteMs = 60 * 1000;
  const hourMs = 60 * minuteMs;
  const dayMs = 24 * hourMs;

  if (ageMs < fiveSecondsMs) {
    return "Updated just now";
  }

  if (ageMs < minuteMs) {
    const ageSeconds = Math.min(55, Math.round(ageMs / fiveSecondsMs) * 5);
    return `Updated ${ageSeconds}s ago`;
  }

  if (ageMs < hourMs) {
    return `Updated ${Math.floor(ageMs / minuteMs)}m ago`;
  }

  if (ageMs < dayMs) {
    return `Updated ${Math.floor(ageMs / hourMs)}h ago`;
  }

  return `Updated ${Math.floor(ageMs / dayMs)}d ago`;
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
      sx={(theme) => ({
        display: "flex",
        flexDirection: "row",
        flexWrap: "nowrap",
        height: 36,
        marginTop: "1px",
        paddingLeft: 2,
        paddingRight: 2,
        backgroundColor: theme.palette.background.paper,
        alignItems: "center",
        borderBottom: `1px solid ${theme.palette.divider}`,
      })}
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
            sx={(theme) => ({
              textDecoration: "none",
              color:
                index === mainNavPageHierarchy.length - 1
                  ? theme.palette.text.primary
                  : theme.palette.text.secondary,
            })}
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
              sx={(theme) => ({
                fontWeight: 500,
                color: theme.palette.text.secondary,
                "&:not(:first-child)": {
                  marginLeft: 1,
                },
              })}
              variant="body2"
            >
              {linkOrText}
            </Typography>
          );
        } else {
          return (
            <React.Fragment key={id}>
              <Typography
                sx={(theme) => ({
                  fontWeight: 500,
                  color: theme.palette.text.secondary,
                  "&:not(:first-child)": {
                    marginLeft: 1,
                  },
                })}
                variant="body2"
              >
                {"/"}
              </Typography>
              <Typography
                sx={(theme) => ({
                  fontWeight: 500,
                  color: theme.palette.text.secondary,
                  "&:not(:first-child)": {
                    marginLeft: 1,
                  },
                })}
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
