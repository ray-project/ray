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
  Theme,
  Tooltip,
} from "@mui/material";
import React, { useContext, useState } from "react";
import { RiExternalLinkLine } from "react-icons/ri";

import { GlobalContext } from "../../App";
import { CollapsibleSection } from "../../common/CollapsibleSection";
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

export type MetricConfig = {
  title: string;
  pathParams: string;
};

export type MetricsSectionConfig = {
  title: string;
  contents: MetricConfig[];
};

export const Metrics = () => {
  const {
    grafanaHost,
    grafanaOrgId,
    prometheusHealth,
    dashboardUids,
    dashboardDatasource,
    sessionName,
    currentTimeZone,
  } = useContext(GlobalContext);

  const grafanaDefaultDashboardUid =
    dashboardUids?.default ?? "rayDefaultDashboard";

  const grafanaOrgIdParam = grafanaOrgId ?? "1";
  const grafanaDefaultDatasource = dashboardDatasource ?? "Prometheus";

  const [viewInGrafanaMenuRef, setViewInGrafanaMenuRef] =
    useState<HTMLButtonElement | null>(null);

  return (
    <div>
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
        <div>
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
              height: 36,
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
                  href={`${grafanaHost}/d/${grafanaDefaultDashboardUid}/?orgId=${grafanaOrgIdParam}&var-datasource=${grafanaDefaultDatasource}`}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Core Dashboard
                </MenuItem>
                {dashboardUids?.["data"] && (
                  <Tooltip title="The Ray Data dashboard has a dropdown to filter the data metrics by Dataset ID">
                    <MenuItem
                      component="a"
                      href={`${grafanaHost}/d/${dashboardUids["data"]}/?orgId=${grafanaOrgIdParam}&var-datasource=${grafanaDefaultDatasource}`}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      Ray Data Dashboard
                    </MenuItem>
                  </Tooltip>
                )}
              </Menu>
            )}
          </Paper>
          <Alert severity="info">
            Tip: You can click on the legend to focus on a specific line in the
            time-series graph. You can use control/cmd + click to filter out a
            line in the time-series graph.
          </Alert>
          <Box sx={{ margin: 1 }}>
            <CollapsibleSection
              key="Default"
              title="Default"
              startExpanded
              sx={{ marginTop: 3 }}
              keepRendered
            >
              <Box
                component="iframe"
                key="Default"
                sx={{ width: "95%", height: "70vh" }}
                src={`${grafanaHost}/d/${grafanaDefaultDashboardUid}/?orgId=${grafanaOrgIdParam}&var-datasource=${grafanaDefaultDatasource}&timezone=${currentTimeZone}&var-SessionName=${sessionName}&kiosk&theme=light`}
                frameBorder="0"
              />
            </CollapsibleSection>
            {dashboardUids?.["data"] && (
              <CollapsibleSection
                key="Data"
                title="Data"
                startExpanded
                sx={{ marginTop: 3 }}
                keepRendered
              >
                <Box
                  component="iframe"
                  key="Data"
                  sx={{ width: "95%", height: "70vh" }}
                  src={`${grafanaHost}/d/${dashboardUids["data"]}/?orgId=${grafanaOrgIdParam}&var-datasource=${grafanaDefaultDatasource}&timezone=${currentTimeZone}&var-SessionName=${sessionName}&kiosk&theme=light`}
                  frameBorder="0"
                />
              </CollapsibleSection>
            )}
          </Box>
        </div>
      )}
    </div>
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
