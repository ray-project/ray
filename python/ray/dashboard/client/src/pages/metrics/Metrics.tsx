import {
  Alert,
  AlertProps,
  Box,
  Button,
  Link,
  Menu,
  MenuItem,
  Paper,
  SxProps,
  Tab,
  Tabs,
  Theme,
  Tooltip,
} from "@mui/material";
import React, { useContext, useState } from "react";
import { RiExternalLinkLine } from "react-icons/ri";

import { useLocalStorage } from "usehooks-ts";
import { GlobalContext } from "../../App";
import { ClassNameProps } from "../../common/props";
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

type DashboardTab = "core" | "data";

// Exported for use by Serve metrics sections (they still use individual panels)
export type MetricConfig = {
  title: string;
  pathParams: string;
};

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

  const [cachedSelectedTab, setCachedSelectedTab] = useLocalStorage<
    DashboardTab | null
  >(`Metrics-selectedTab`, null);

  const [selectedTab, setSelectedTab] = useState<DashboardTab>(
    cachedSelectedTab ?? "core",
  );

  const [viewInGrafanaMenuRef, setViewInGrafanaMenuRef] =
    useState<HTMLButtonElement | null>(null);

  // Build the dashboard URL based on selected tab
  const buildDashboardUrl = (tab: DashboardTab): string => {
    const dashboardUid =
      tab === "data" ? grafanaDataDashboardUid : grafanaDefaultDashboardUid;
    const baseParams = `orgId=${grafanaOrgIdParam}&theme=light&kiosk=tv`;
    const variableParams = `&var-SessionName=${sessionName}&var-datasource=${grafanaDefaultDatasource}${grafanaClusterFilterParam}`;
    const timezoneParam = `&timezone=${currentTimeZone}`;
    // Use default time range (last 5 minutes) and refresh (5 seconds)
    const timeRangeParams = `&from=now-5m&to=now`;
    const refreshParams = `&refresh=5s`;

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
              justifyContent: "space-between",
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
                borderBottom: "none",
              }}
            >
              <Tab label="Core" value="core" />
              {grafanaDataDashboardUid && (
                <Tab label="Ray Data" value="data" />
              )}
            </Tabs>
            <Box sx={{ paddingRight: 2 }}>
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
            </Box>
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
