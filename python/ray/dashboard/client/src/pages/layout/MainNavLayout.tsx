import {
  Box,
  IconButton,
  Link,
  MenuItem,
  Select,
  SelectChangeEvent,
  Tooltip,
  Typography,
} from "@mui/material";
import React, { useContext, useEffect, useState } from "react";
import { RiBookMarkLine, RiFeedbackLine } from "react-icons/ri/";
import { Outlet, Link as RouterLink } from "react-router-dom";
import { GlobalContext } from "../../App";
import { formatTimeZone } from "../../common/formatUtils";
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

  const [timezone, setTimezone] = useState<string>(() => {
    const currentTimezone =
      localStorage.getItem("timezone") ||
      Intl.DateTimeFormat().resolvedOptions().timeZone;
    formatTimeZone(currentTimezone);
    return currentTimezone;
  });

  useEffect(() => {
    localStorage.setItem("timezone", timezone);
  }, [timezone]);

  const handleTimezoneChange = (event: SelectChangeEvent<string>) => {
    setTimezone(event.target.value);
    window.location.reload();
  };

  const timezones = [
    { label: "(GMT-12:00) International Date Line West", value: "Etc/GMT+12" },
    { label: "(GMT-11:00) American Samoa", value: "Pacific/Pago_Pago" },
    { label: "(GMT-11:00) Midway Island", value: "Pacific/Midway" },
    { label: "(GMT-10:00) Hawaii", value: "Pacific/Honolulu" },
    { label: "(GMT-09:00) Alaska", value: "America/Anchorage" },
    {
      label: "(GMT-08:00) Pacific Time (US & Canada)",
      value: "America/Los_Angeles",
    },
    { label: "(GMT-08:00) Tijuana", value: "America/Tijuana" },
    { label: "(GMT-07:00) Arizona", value: "America/Phoenix" },
    { label: "(GMT-07:00) Mazatlan", value: "America/Mazatlan" },
    {
      label: "(GMT-07:00) Mountain Time (US & Canada)",
      value: "America/Denver",
    },
    { label: "(GMT-06:00) Central America", value: "America/Guatemala" },
    {
      label: "(GMT-06:00) Central Time (US & Canada)",
      value: "America/Chicago",
    },
    { label: "(GMT-06:00) Chihuahua", value: "America/Chihuahua" },
    { label: "(GMT-06:00) Guadalajara", value: "America/Guadalajara" },
    { label: "(GMT-06:00) Mexico City", value: "America/Mexico_City" },
    { label: "(GMT-06:00) Monterrey", value: "America/Monterrey" },
    { label: "(GMT-06:00) Saskatchewan", value: "America/Regina" },
    { label: "(GMT-05:00) Bogota", value: "America/Bogota" },
    {
      label: "(GMT-05:00) Eastern Time (US & Canada)",
      value: "America/New_York",
    },
    {
      label: "(GMT-05:00) Indiana (East)",
      value: "America/Indiana/Indianapolis",
    },
    { label: "(GMT-05:00) Lima", value: "America/Lima" },
    { label: "(GMT-05:00) Quito", value: "America/Guayaquil" },
    { label: "(GMT-04:00) Atlantic Time (Canada)", value: "America/Halifax" },
    { label: "(GMT-04:00) Caracas", value: "America/Caracas" },
    { label: "(GMT-04:00) Georgetown", value: "America/Guyana" },
    { label: "(GMT-04:00) La Paz", value: "America/La_Paz" },
    { label: "(GMT-04:00) Puerto Rico", value: "America/Puerto_Rico" },
    { label: "(GMT-04:00) Santiago", value: "America/Santiago" },
    { label: "(GMT-03:30) Newfoundland", value: "America/St_Johns" },
    { label: "(GMT-03:00) Brasilia", value: "America/Sao_Paulo" },
    {
      label: "(GMT-03:00) Buenos Aires",
      value: "America/Argentina/Buenos_Aires",
    },
    { label: "(GMT-03:00) Montevideo", value: "America/Montevideo" },
    { label: "(GMT-02:00) Greenland", value: "America/Godthab" },
    { label: "(GMT-02:00) Mid-Atlantic", value: "Etc/GMT+2" },
    { label: "(GMT-01:00) Azores", value: "Atlantic/Azores" },
    { label: "(GMT-01:00) Cape Verde Is.", value: "Atlantic/Cape_Verde" },
    { label: "(GMT+00:00) Edinburgh", value: "Europe/London" },
    { label: "(GMT+00:00) Lisbon", value: "Europe/Lisbon" },
    { label: "(GMT+00:00) London", value: "Europe/London" },
    { label: "(GMT+00:00) Monrovia", value: "Africa/Monrovia" },
    { label: "(GMT+00:00) UTC", value: "Etc/UTC" },
    { label: "(GMT+01:00) Amsterdam", value: "Europe/Amsterdam" },
    { label: "(GMT+01:00) Belgrade", value: "Europe/Belgrade" },
    { label: "(GMT+01:00) Berlin", value: "Europe/Berlin" },
    { label: "(GMT+01:00) Brussels", value: "Europe/Brussels" },
    { label: "(GMT+01:00) Budapest", value: "Europe/Budapest" },
    { label: "(GMT+01:00) Copenhagen", value: "Europe/Copenhagen" },
    { label: "(GMT+01:00) Madrid", value: "Europe/Madrid" },
    { label: "(GMT+01:00) Paris", value: "Europe/Paris" },
    { label: "(GMT+01:00) Prague", value: "Europe/Prague" },
    { label: "(GMT+01:00) Rome", value: "Europe/Rome" },
    { label: "(GMT+01:00) Sarajevo", value: "Europe/Sarajevo" },
    { label: "(GMT+01:00) Stockholm", value: "Europe/Stockholm" },
    { label: "(GMT+01:00) Vienna", value: "Europe/Vienna" },
    { label: "(GMT+01:00) Warsaw", value: "Europe/Warsaw" },
    { label: "(GMT+01:00) West Central Africa", value: "Africa/Lagos" },
    { label: "(GMT+02:00) Amman", value: "Asia/Amman" },
    { label: "(GMT+02:00) Athens", value: "Europe/Athens" },
    { label: "(GMT+02:00) Beirut", value: "Asia/Beirut" },
    { label: "(GMT+02:00) Bucharest", value: "Europe/Bucharest" },
    { label: "(GMT+02:00) Cairo", value: "Africa/Cairo" },
    { label: "(GMT+02:00) Harare", value: "Africa/Harare" },
    { label: "(GMT+02:00) Helsinki", value: "Europe/Helsinki" },
    { label: "(GMT+02:00) Istanbul", value: "Europe/Istanbul" },
    { label: "(GMT+02:00) Jerusalem", value: "Asia/Jerusalem" },
    { label: "(GMT+02:00) Kyiv", value: "Europe/Kiev" },
    { label: "(GMT+02:00) Minsk", value: "Europe/Minsk" },
    { label: "(GMT+02:00) Riga", value: "Europe/Riga" },
    { label: "(GMT+02:00) Sofia", value: "Europe/Sofia" },
    { label: "(GMT+02:00) Tallinn", value: "Europe/Tallinn" },
    { label: "(GMT+02:00) Vilnius", value: "Europe/Vilnius" },
    { label: "(GMT+03:00) Baghdad", value: "Asia/Baghdad" },
    { label: "(GMT+03:00) Kuwait", value: "Asia/Kuwait" },
    { label: "(GMT+03:00) Moscow", value: "Europe/Moscow" },
    { label: "(GMT+03:00) Nairobi", value: "Africa/Nairobi" },
    { label: "(GMT+03:00) Riyadh", value: "Asia/Riyadh" },
    { label: "(GMT+03:30) Tehran", value: "Asia/Tehran" },
    { label: "(GMT+04:00) Abu Dhabi", value: "Asia/Dubai" },
    { label: "(GMT+04:00) Baku", value: "Asia/Baku" },
    { label: "(GMT+04:00) Muscat", value: "Asia/Muscat" },
    { label: "(GMT+04:00) Tbilisi", value: "Asia/Tbilisi" },
    { label: "(GMT+04:00) Yerevan", value: "Asia/Yerevan" },
    { label: "(GMT+04:30) Kabul", value: "Asia/Kabul" },
    { label: "(GMT+05:00) Islamabad", value: "Asia/Karachi" },
    { label: "(GMT+05:00) Tashkent", value: "Asia/Tashkent" },
    { label: "(GMT+05:30) Chennai", value: "Asia/Kolkata" },
    { label: "(GMT+05:30) Kolkata", value: "Asia/Kolkata" },
    { label: "(GMT+05:30) Mumbai", value: "Asia/Kolkata" },
    { label: "(GMT+05:30) New Delhi", value: "Asia/Kolkata" },
    { label: "(GMT+05:45) Kathmandu", value: "Asia/Kathmandu" },
    { label: "(GMT+06:00) Almaty", value: "Asia/Almaty" },
    { label: "(GMT+06:00) Dhaka", value: "Asia/Dhaka" },
    { label: "(GMT+06:00) Yekaterinburg", value: "Asia/Yekaterinburg" },
    { label: "(GMT+06:30) Yangon (Rangoon)", value: "Asia/Yangon" },
    { label: "(GMT+07:00) Bangkok", value: "Asia/Bangkok" },
    { label: "(GMT+07:00) Hanoi", value: "Asia/Hanoi" },
    { label: "(GMT+07:00) Jakarta", value: "Asia/Jakarta" },
    { label: "(GMT+07:00) Novosibirsk", value: "Asia/Novosibirsk" },
    { label: "(GMT+08:00) Beijing", value: "Asia/Shanghai" },
    { label: "(GMT+08:00) Chongqing", value: "Asia/Chongqing" },
    { label: "(GMT+08:00) Hong Kong", value: "Asia/Hong_Kong" },
    { label: "(GMT+08:00) Krasnoyarsk", value: "Asia/Krasnoyarsk" },
    { label: "(GMT+08:00) Kuala Lumpur", value: "Asia/Kuala_Lumpur" },
    { label: "(GMT+08:00) Perth", value: "Australia/Perth" },
    { label: "(GMT+08:00) Singapore", value: "Asia/Singapore" },
    { label: "(GMT+08:00) Taipei", value: "Asia/Taipei" },
    { label: "(GMT+08:00) Ulaan Bataar", value: "Asia/Ulaanbaatar" },
    { label: "(GMT+08:00) Urumqi", value: "Asia/Urumqi" },
    { label: "(GMT+09:00) Irkutsk", value: "Asia/Irkutsk" },
    { label: "(GMT+09:00) Osaka", value: "Asia/Tokyo" },
    { label: "(GMT+09:00) Sapporo", value: "Asia/Tokyo" },
    { label: "(GMT+09:00) Seoul", value: "Asia/Seoul" },
    { label: "(GMT+09:00) Tokyo", value: "Asia/Tokyo" },
    { label: "(GMT+09:30) Adelaide", value: "Australia/Adelaide" },
    { label: "(GMT+09:30) Darwin", value: "Australia/Darwin" },
    { label: "(GMT+10:00) Brisbane", value: "Australia/Brisbane" },
    { label: "(GMT+10:00) Canberra", value: "Australia/Sydney" },
    { label: "(GMT+10:00) Guam", value: "Pacific/Guam" },
    { label: "(GMT+10:00) Hobart", value: "Australia/Hobart" },
    { label: "(GMT+10:00) Melbourne", value: "Australia/Melbourne" },
    { label: "(GMT+10:00) Port Moresby", value: "Pacific/Port_Moresby" },
    { label: "(GMT+10:00) Sydney", value: "Australia/Sydney" },
    { label: "(GMT+11:00) Magadan", value: "Asia/Magadan" },
    { label: "(GMT+11:00) New Caledonia", value: "Pacific/Noumea" },
    { label: "(GMT+11:00) Solomon Is.", value: "Pacific/Guadalcanal" },
    { label: "(GMT+12:00) Auckland", value: "Pacific/Auckland" },
    { label: "(GMT+12:00) Fiji", value: "Pacific/Fiji" },
    { label: "(GMT+12:00) Kamchatka", value: "Asia/Kamchatka" },
    { label: "(GMT+12:00) Marshall Is.", value: "Pacific/Majuro" },
    { label: "(GMT+12:00) Wellington", value: "Pacific/Auckland" },
    { label: "(GMT+13:00) Nuku'alofa", value: "Pacific/Tongatapu" },
    { label: "(GMT+13:00) Samoa", value: "Pacific/Apia" },
    { label: "(GMT+13:00) Tokelau Is.", value: "Pacific/Fakaofo" },
  ];
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
      <Box sx={{ marginRight: 2 }}>
        <Select<string>
          value={timezone}
          onChange={handleTimezoneChange}
          displayEmpty
          inputProps={{ "aria-label": "Timezone select" }}
          size="small"
        >
          {timezones.map((tz) => (
            <MenuItem key={tz.value} value={tz.value}>
              {tz.label}
            </MenuItem>
          ))}
        </Select>
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
