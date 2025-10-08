import {
  Alert,
  AlertProps,
  Box,
  Button,
  InputAdornment,
  Link,
  Menu,
  MenuItem,
  Paper,
  SxProps,
  Tab,
  Tabs,
  TextField,
  Theme,
  Tooltip,
} from "@mui/material";
import React, { useContext, useEffect, useState } from "react";
import { BiRefresh, BiTime } from "react-icons/bi";
import { RiExternalLinkLine } from "react-icons/ri";

import { useLocalStorage } from "usehooks-ts";
import { GlobalContext } from "../../App";
import { ClassNameProps } from "../../common/props";
import { HelpInfo } from "../../components/Tooltip";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { MAIN_NAV_HEIGHT } from "../layout/MainNavLayout";

export enum RefreshOptions {
  OFF = "off",
  FIVE_SECONDS = "5s",
  TEN_SECONDS = "10s",
  THIRTY_SECONDS = "30s",
  ONE_MIN = "1m",
  FIVE_MINS = "5m",
  FIFTEEN_MINS = "15m",
  THIRTY_MINS = "30m",
  ONE_HOUR = "1h",
  TWO_HOURS = "2h",
  ONE_DAY = "1d",
}

export enum TimeRangeOptions {
  FIVE_MINS = "Last 5 minutes",
  THIRTY_MINS = "Last 30 minutes",
  ONE_HOUR = "Last 1 hour",
  THREE_HOURS = "Last 3 hours",
  SIX_HOURS = "Last 6 hours",
  TWELVE_HOURS = "Last 12 hours",
  ONE_DAY = "Last 1 day",
  TWO_DAYS = "Last 2 days",
  SEVEN_DAYS = "Last 7 days",
}

export const REFRESH_VALUE: Record<RefreshOptions, string> = {
  [RefreshOptions.OFF]: "",
  [RefreshOptions.FIVE_SECONDS]: "5s",
  [RefreshOptions.TEN_SECONDS]: "10s",
  [RefreshOptions.THIRTY_SECONDS]: "30s",
  [RefreshOptions.ONE_MIN]: "1m",
  [RefreshOptions.FIVE_MINS]: "5m",
  [RefreshOptions.FIFTEEN_MINS]: "15m",
  [RefreshOptions.THIRTY_MINS]: "30m",
  [RefreshOptions.ONE_HOUR]: "1h",
  [RefreshOptions.TWO_HOURS]: "2h",
  [RefreshOptions.ONE_DAY]: "1d",
};

export const TIME_RANGE_TO_FROM_VALUE: Record<TimeRangeOptions, string> = {
  [TimeRangeOptions.FIVE_MINS]: "now-5m",
  [TimeRangeOptions.THIRTY_MINS]: "now-30m",
  [TimeRangeOptions.ONE_HOUR]: "now-1h",
  [TimeRangeOptions.THREE_HOURS]: "now-3h",
  [TimeRangeOptions.SIX_HOURS]: "now-6h",
  [TimeRangeOptions.TWELVE_HOURS]: "now-12h",
  [TimeRangeOptions.ONE_DAY]: "now-1d",
  [TimeRangeOptions.TWO_DAYS]: "now-2d",
  [TimeRangeOptions.SEVEN_DAYS]: "now-7d",
};

const CONTROL_TOOLBAR_HEIGHT = 36;
const TAB_BAR_HEIGHT = 48;

type DashboardTab = "core" | "data";

export const Metrics = () => {
  const {
    grafanaHost,
    grafanaOrgId,
    grafanaClusterFilter,
    prometheusHealth,
    dashboardUids,
    dashboardDatasource,
    sessionName,
    currentTimeZone,
  } = useContext(GlobalContext);

  const grafanaDefaultDashboardUid =
    dashboardUids?.default ?? "rayDefaultDashboard";
  const grafanaDataDashboardUid = dashboardUids?.data;

  const grafanaOrgIdParam = grafanaOrgId ?? "1";
  const grafanaDefaultDatasource = dashboardDatasource ?? "Prometheus";
  const grafanaClusterFilterParam = grafanaClusterFilter
    ? `&var-Cluster=${grafanaClusterFilter}`
    : "";

  const [cachedRefreshOptionStr, setCachedRefreshOptionStr] = useLocalStorage<
    string | null
  >(`Metrics-refreshOption`, null);
  const cachedRefreshOption = cachedRefreshOptionStr
    ? (cachedRefreshOptionStr as RefreshOptions)
    : undefined;

  const [cachedTimeRangeOptionStr, setCachedTimeRangeOptionStr] =
    useLocalStorage<string | null>(`Metrics-timeRangeOption`, null);

  const cachedTimeRangeOption = cachedTimeRangeOptionStr
    ? (cachedTimeRangeOptionStr as TimeRangeOptions)
    : undefined;

  const [cachedSelectedTab, setCachedSelectedTab] = useLocalStorage<
    DashboardTab | null
  >(`Metrics-selectedTab`, null);

  const [refreshOption, setRefreshOption] = useState<RefreshOptions>(
    cachedRefreshOption ?? RefreshOptions.FIVE_SECONDS,
  );

  const [timeRangeOption, setTimeRangeOption] = useState<TimeRangeOptions>(
    cachedTimeRangeOption ?? TimeRangeOptions.FIVE_MINS,
  );

  const [selectedTab, setSelectedTab] = useState<DashboardTab>(
    cachedSelectedTab ?? "core",
  );

  const [refresh, setRefresh] = useState<string | null>(null);

  const [[from, to], setTimeRange] = useState<[string | null, string | null]>([
    null,
    null,
  ]);

  useEffect(() => {
    setRefresh(REFRESH_VALUE[refreshOption]);
  }, [refreshOption]);

  useEffect(() => {
    const from = TIME_RANGE_TO_FROM_VALUE[timeRangeOption];
    setTimeRange([from, "now"]);
  }, [timeRangeOption]);

  const [viewInGrafanaMenuRef, setViewInGrafanaMenuRef] =
    useState<HTMLButtonElement | null>(null);

  const fromParam = from !== null ? `&from=${from}` : "";
  const toParam = to !== null ? `&to=${to}` : "";
  const timeRangeParams = `${fromParam}${toParam}`;

  const refreshParams = refresh ? `&refresh=${refresh}` : "";

  // Build the dashboard URL based on selected tab
  const buildDashboardUrl = (tab: DashboardTab): string => {
    const dashboardUid =
      tab === "data" ? grafanaDataDashboardUid : grafanaDefaultDashboardUid;
    const baseParams = `orgId=${grafanaOrgIdParam}&theme=light&kiosk=tv`;
    const variableParams = `&var-SessionName=${sessionName}&var-datasource=${grafanaDefaultDatasource}${grafanaClusterFilterParam}`;
    const timezoneParam = `&timezone=${currentTimeZone}`;

    return `${grafanaHost}/d/${dashboardUid}/?${baseParams}${refreshParams}${timeRangeParams}${timezoneParam}${variableParams}`;
  };

  const currentDashboardUrl = buildDashboardUrl(selectedTab);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: `calc(100vh - ${MAIN_NAV_HEIGHT}px)`,
      }}
    >
      <MainNavPageInfo
        pageInfo={{
          id: "metrics",
          title: "Metrics",
          path: "/metrics",
        }}
      />
      {grafanaHost === undefined || !prometheusHealth ? (
        <GrafanaNotRunningAlert sx={{ marginTop: "30px" }} />
      ) : (
        <>
          <Paper
            sx={{
              position: "sticky",
              top: MAIN_NAV_HEIGHT,
              width: "100%",
              display: "flex",
              flexDirection: "row",
              alignItems: "center",
              justifyContent: "flex-end",
              padding: 1,
              boxShadow: "0px 1px 0px #D2DCE6",
              zIndex: 1,
              height: CONTROL_TOOLBAR_HEIGHT,
              flexShrink: 0,
            }}
          >
            <Button
              onClick={({ currentTarget }) => {
                setViewInGrafanaMenuRef(currentTarget);
              }}
              endIcon={<RiExternalLinkLine />}
            >
              View in Grafana
            </Button>
            {viewInGrafanaMenuRef && (
              <Menu
                open
                anchorEl={viewInGrafanaMenuRef}
                anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
                transformOrigin={{ vertical: "top", horizontal: "right" }}
                onClose={() => {
                  setViewInGrafanaMenuRef(null);
                }}
              >
                <MenuItem
                  component="a"
                  href={`${grafanaHost}/d/${grafanaDefaultDashboardUid}/?orgId=${grafanaOrgIdParam}&var-datasource=${grafanaDefaultDatasource}${grafanaClusterFilterParam}`}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Core Dashboard
                </MenuItem>
                {grafanaDataDashboardUid && (
                  <Tooltip title="The Ray Data dashboard has a dropdown to filter the data metrics by Dataset ID">
                    <MenuItem
                      component="a"
                      href={`${grafanaHost}/d/${grafanaDataDashboardUid}/?orgId=${grafanaOrgIdParam}&var-datasource=${grafanaDefaultDatasource}${grafanaClusterFilterParam}`}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      Ray Data Dashboard
                    </MenuItem>
                  </Tooltip>
                )}
              </Menu>
            )}
            <TextField
              sx={{ marginLeft: 2, width: 80 }}
              select
              size="small"
              value={refreshOption}
              onChange={({ target: { value } }) => {
                setRefreshOption(value as RefreshOptions);
                setCachedRefreshOptionStr(value);
              }}
              variant="standard"
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <BiRefresh style={{ fontSize: 25, paddingBottom: 5 }} />
                  </InputAdornment>
                ),
              }}
            >
              {Object.entries(RefreshOptions).map(([key, value]) => (
                <MenuItem key={key} value={value}>
                  {value}
                </MenuItem>
              ))}
            </TextField>
            <HelpInfo>Auto-refresh interval</HelpInfo>
            <TextField
              sx={{ marginLeft: 2, width: 140 }}
              select
              size="small"
              value={timeRangeOption}
              onChange={({ target: { value } }) => {
                setTimeRangeOption(value as TimeRangeOptions);
                setCachedTimeRangeOptionStr(value);
              }}
              variant="standard"
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <BiTime style={{ fontSize: 22, paddingBottom: 5 }} />
                  </InputAdornment>
                ),
              }}
            >
              {Object.entries(TimeRangeOptions).map(([key, value]) => (
                <MenuItem key={key} value={value}>
                  {value}
                </MenuItem>
              ))}
            </TextField>
            <HelpInfo>Time range picker</HelpInfo>
          </Paper>
          <Paper
            sx={{
              position: "sticky",
              top: MAIN_NAV_HEIGHT + CONTROL_TOOLBAR_HEIGHT,
              width: "100%",
              boxShadow: "0px 1px 0px #D2DCE6",
              zIndex: 1,
              flexShrink: 0,
            }}
          >
            <Tabs
              value={selectedTab}
              onChange={(_, newValue) => {
                setSelectedTab(newValue as DashboardTab);
                setCachedSelectedTab(newValue as DashboardTab);
              }}
              sx={{
                borderBottom: (theme) => `1px solid ${theme.palette.divider}`,
              }}
            >
              <Tab label="Core" value="core" />
              {grafanaDataDashboardUid && (
                <Tab label="Ray Data" value="data" />
              )}
            </Tabs>
          </Paper>
          <Box
            sx={{
              flex: 1,
              overflow: "hidden",
              width: "100%",
            }}
          >
            <Box
              component="iframe"
              title={selectedTab === "data" ? "Ray Data Dashboard" : "Core Dashboard"}
              src={currentDashboardUrl}
              sx={{
                width: "100%",
                height: "100%",
                border: "none",
              }}
            />
          </Box>
        </>
      )}
    </Box>
  );
};


export type GrafanaNotRunningAlertProps = {
  severity?: AlertProps["severity"];
  sx?: SxProps<Theme>;
} & ClassNameProps;

export const GrafanaNotRunningAlert = ({
  className,
  severity = "warning",
  sx,
}: GrafanaNotRunningAlertProps) => {
  const { grafanaHost, prometheusHealth } = useContext(GlobalContext);
  return grafanaHost === undefined || !prometheusHealth ? (
    <Alert className={className} sx={sx} severity={severity}>
      <Box component="span" sx={{ fontWeight: 500 }}>
        Set up Prometheus and Grafana for better Ray Dashboard experience
      </Box>
      <br />
      <br />
      Time-series charts are hidden because either Prometheus or Grafana server
      is not detected. Follow{" "}
      <Link
        href="https://docs.ray.io/en/latest/cluster/metrics.html"
        target="_blank"
        rel="noreferrer"
      >
        these instructions
      </Link>{" "}
      to set them up and refresh this page.
    </Alert>
  ) : null;
};
