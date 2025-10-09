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
  TextField,
  Theme,
  Tooltip,
} from "@mui/material";
import React, { useContext, useEffect, useState } from "react";
import { BiRefresh, BiTime } from "react-icons/bi";
import { RiExternalLinkLine } from "react-icons/ri";

import { useLocalStorage } from "usehooks-ts";
import { GlobalContext } from "../../App";
import { CollapsibleSection } from "../../common/CollapsibleSection";
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

export type MetricConfig = {
  title: string;
  pathParams: string;
};

export type MetricsSectionConfig = {
  title: string;
  contents: MetricConfig[];
};

// NOTE: please keep the titles here in sync with dashboard/modules/metrics/dashboards/default_dashboard_panels.py
const METRICS_CONFIG: MetricsSectionConfig[] = [
  {
    title: "Tasks and Actors",
    contents: [
      {
        title: "Scheduler Task State",
        pathParams: "theme=light&panelId=26",
      },
      {
        title: "Requested Live Tasks by Name",
        pathParams: "theme=light&panelId=35",
      },
      {
        title: "Running Tasks by Name",
        pathParams: "theme=light&panelId=38",
      },
      {
        title: "Scheduler Actor State",
        pathParams: "theme=light&panelId=33",
      },
      {
        title: "Requested Live Actors by Name",
        pathParams: "theme=light&panelId=36",
      },
      {
        title: "Out of Memory Failures by Name",
        pathParams: "theme=light&panelId=44",
      },
    ],
  },
  {
    title: "Ray Resource Usage",
    contents: [
      {
        title: "Scheduler CPUs (logical slots)",
        pathParams: "theme=light&panelId=27",
      },
      {
        title: "Scheduler GPUs (logical slots)",
        pathParams: "theme=light&panelId=28",
      },
      {
        title: "Object Store Memory",
        pathParams: "theme=light&panelId=29",
      },
      {
        title: "Placement Groups",
        pathParams: "theme=light&panelId=40",
      },
    ],
  },
  {
    title: "Hardware Utilization",
    contents: [
      {
        title: "Node Count",
        pathParams: "theme=light&panelId=24",
      },
      {
        title: "Node CPU (hardware utilization)",
        pathParams: "theme=light&panelId=2",
      },
      {
        title: "Node Memory (heap + object store)",
        pathParams: "theme=light&panelId=4",
      },
      {
        title: "Node Memory Percentage (heap + object store)",
        pathParams: "theme=light&panelId=48",
      },
      {
        title: "Node GPU (hardware utilization)",
        pathParams: "theme=light&panelId=8",
      },
      {
        title: "Node GPU Memory (GRAM)",
        pathParams: "theme=light&panelId=18",
      },
      {
        title: "Node TPU (tensorcore utilization)",
        pathParams: "theme=light&panelId=50",
      },
      {
        title: "Node TPU (high bandwidth memory utilization)",
        pathParams: "theme=light&panelId=51",
      },
      {
        title: "Node TPU (duty cycle)",
        pathParams: "theme=light&panelId=52",
      },
      {
        title: "Node TPU (memory used)",
        pathParams: "theme=light&panelId=53",
      },
      {
        title: "Node Disk",
        pathParams: "theme=light&panelId=6",
      },
      {
        title: "Node Disk IO Speed",
        pathParams: "theme=light&panelId=32",
      },
      {
        title: "Node Network",
        pathParams: "theme=light&panelId=20",
      },
      {
        title: "Node CPU by Component",
        pathParams: "theme=light&panelId=37",
      },
      {
        title: "Node Memory by Component",
        pathParams: "theme=light&panelId=34",
      },
      {
        title: "Node GPU by Component",
        pathParams: "orgId=1&theme=light&panelId=45",
      },
      {
        title: "Node GPU Memory by Component",
        pathParams: "orgId=1&theme=light&panelId=46",
      },
    ],
  },
];

const DATA_METRICS_CONFIG: MetricsSectionConfig[] = [
  {
    title: "Ray Data Metrics (Overview)",
    contents: [
      {
        title: "Bytes Spilled",
        pathParams: "theme=light&panelId=1",
      },
      {
        title: "Bytes Freed",
        pathParams: "theme=light&panelId=3",
      },
      {
        title: "Object Store Memory",
        pathParams: "theme=light&panelId=4",
      },
      {
        title: "CPUs (logical slots)",
        pathParams: "theme=light&panelId=5",
      },
      {
        title: "GPUs (logical slots)",
        pathParams: "theme=light&panelId=6",
      },
      {
        title: "Bytes Outputted",
        pathParams: "theme=light&panelId=7",
      },
      {
        title: "Rows Outputted",
        pathParams: "theme=light&panelId=11",
      },
    ],
  },
  {
    title: "Ray Data Metrics (Inputs)",
    contents: [
      {
        title: "Input Blocks Received by Operator",
        pathParams: "theme=light&panelId=17",
      },
      {
        title: "Input Blocks Processed by Tasks",
        pathParams: "theme=light&panelId=19",
      },
      {
        title: "Input Bytes Processed by Tasks",
        pathParams: "theme=light&panelId=20",
      },
      {
        title: "Input Bytes Submitted to Tasks",
        pathParams: "theme=light&panelId=21",
      },
    ],
  },
  {
    title: "Ray Data Metrics (Outputs)",
    contents: [
      {
        title: "Blocks Generated by Tasks",
        pathParams: "theme=light&panelId=22",
      },
      {
        title: "Bytes Generated by Tasks",
        pathParams: "theme=light&panelId=23",
      },
      {
        title: "Rows Generated by Tasks",
        pathParams: "theme=light&panelId=24",
      },
      {
        title: "Output Blocks Taken by Downstream Operators",
        pathParams: "theme=light&panelId=25",
      },
      {
        title: "Output Bytes Taken by Downstream Operators",
        pathParams: "theme=light&panelId=26",
      },
    ],
  },
  {
    title: "Ray Data Metrics (Tasks)",
    contents: [
      {
        title: "Submitted Tasks",
        pathParams: "theme=light&panelId=29",
      },
      {
        title: "Running Tasks",
        pathParams: "theme=light&panelId=30",
      },
      {
        title: "Tasks with output blocks",
        pathParams: "theme=light&panelId=31",
      },
      {
        title: "Finished Tasks",
        pathParams: "theme=light&panelId=32",
      },
      {
        title: "Failed Tasks",
        pathParams: "theme=light&panelId=33",
      },
      {
        title: "Block Generation Time",
        pathParams: "theme=light&panelId=8",
      },
      {
        title: "Task Submission Backpressure Time",
        pathParams: "theme=light&panelId=37",
      },
      {
        title: "Task Completion Time",
        pathParams: "theme=light&panelId=38",
      },
    ],
  },
  {
    title: "Ray Data Metrics (Object Store Memory)",
    contents: [
      {
        title: "Operator Internal Inqueue Size (Blocks)",
        pathParams: "theme=light&panelId=13",
      },
      {
        title: "Operator Internal Inqueue Size (Bytes)",
        pathParams: "theme=light&panelId=14",
      },
      {
        title: "Operator Internal Outqueue Size (Blocks)",
        pathParams: "theme=light&panelId=15",
      },
      {
        title: "Operator Internal Outqueue Size (Bytes)",
        pathParams: "theme=light&panelId=16",
      },
      {
        title: "Size of Blocks used in Pending Tasks (Bytes)",
        pathParams: "theme=light&panelId=34",
      },
      {
        title: "Freed Memory in Object Store (Bytes)",
        pathParams: "theme=light&panelId=35",
      },
      {
        title: "Spilled Memory in Object Store (Bytes)",
        pathParams: "theme=light&panelId=36",
      },
    ],
  },
  {
    title: "Ray Data Metrics (Iteration)",
    contents: [
      {
        title: "Iteration Initialization Time",
        pathParams: "theme=light&panelId=12",
      },
      {
        title: "Iteration Blocked Time",
        pathParams: "theme=light&panelId=9",
      },
      {
        title: "Iteration User Time",
        pathParams: "theme=light&panelId=10",
      },
    ],
  },
  // Add metrics with `metrics_group: "misc"` here.
  // {
  //   title: "Ray Data Metrics (Miscellaneous)",
  //   contents: [],
  // },
];

export const Metrics = () => {
  const {
    grafanaHost,
    grafanaOrgId,
    grafanaClusterFilter,
    prometheusHealth,
    dashboardUids,
    dashboardDatasource,
  } = useContext(GlobalContext);

  const grafanaDefaultDashboardUid =
    dashboardUids?.default ?? "rayDefaultDashboard";

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

  const [refreshOption, setRefreshOption] = useState<RefreshOptions>(
    cachedRefreshOption ?? RefreshOptions.FIVE_SECONDS,
  );

  const [timeRangeOption, setTimeRangeOption] = useState<TimeRangeOptions>(
    cachedTimeRangeOption ?? TimeRangeOptions.FIVE_MINS,
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
                  href={`${grafanaHost}/d/${grafanaDefaultDashboardUid}/?orgId=${grafanaOrgId}&var-datasource=${grafanaDefaultDatasource}${grafanaClusterFilterParam}`}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Core Dashboard
                </MenuItem>
                {dashboardUids?.["data"] && (
                  <Tooltip title="The Ray Data dashboard has a dropdown to filter the data metrics by Dataset ID">
                    <MenuItem
                      component="a"
                      href={`${grafanaHost}/d/${dashboardUids["data"]}/?orgId=${grafanaOrgId}&var-datasource=${grafanaDefaultDatasource}${grafanaClusterFilterParam}`}
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
          <Alert severity="info">
            Tip: You can click on the legend to focus on a specific line in the
            time-series graph. You can use control/cmd + click to filter out a
            line in the time-series graph.
          </Alert>
          <Box sx={{ margin: 1 }}>
            {METRICS_CONFIG.map((config) => (
              <MetricsSection
                key={config.title}
                metricConfig={config}
                refreshParams={refreshParams}
                timeRangeParams={timeRangeParams}
                grafanaOrgId={grafanaOrgIdParam}
                dashboardUid={grafanaDefaultDashboardUid}
                dashboardDatasource={grafanaDefaultDatasource}
                grafanaClusterFilterParam={grafanaClusterFilterParam}
              />
            ))}
            {dashboardUids?.["data"] &&
              DATA_METRICS_CONFIG.map((config) => (
                <MetricsSection
                  key={config.title}
                  metricConfig={config}
                  refreshParams={refreshParams}
                  timeRangeParams={timeRangeParams}
                  grafanaOrgId={grafanaOrgIdParam}
                  dashboardUid={dashboardUids["data"]}
                  dashboardDatasource={grafanaDefaultDatasource}
                  grafanaClusterFilterParam={grafanaClusterFilterParam}
                />
              ))}
          </Box>
        </div>
      )}
    </div>
  );
};

type MetricsSectionProps = {
  metricConfig: MetricsSectionConfig;
  refreshParams: string;
  timeRangeParams: string;
  grafanaOrgId: string;
  grafanaClusterFilterParam: string;
  dashboardUid: string;
  dashboardDatasource: string;
};

const MetricsSection = ({
  metricConfig: { title, contents },
  refreshParams,
  timeRangeParams,
  grafanaOrgId,
  grafanaClusterFilterParam,
  dashboardUid,
  dashboardDatasource,
}: MetricsSectionProps) => {
  const { grafanaHost, sessionName, currentTimeZone } =
    useContext(GlobalContext);

  return (
    <CollapsibleSection
      key={title}
      title={title}
      startExpanded
      sx={{ marginTop: 3 }}
      keepRendered
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          flexWrap: "wrap",
          gap: 3,
          marginTop: 2,
        }}
      >
        {contents.map(({ title, pathParams }) => {
          const path =
            `/d-solo/${dashboardUid}?${pathParams}&orgId=${grafanaOrgId}` +
            `${refreshParams}&timezone=${currentTimeZone}${timeRangeParams}&var-SessionName=${sessionName}&var-datasource=${dashboardDatasource}${grafanaClusterFilterParam}`;
          return (
            <Paper
              key={pathParams}
              sx={(theme) => ({
                width: "100%",
                height: 400,
                overflow: "hidden",
                [theme.breakpoints.up("md")]: {
                  // Calculate max width based on 1/3 of the total width minus padding between cards
                  width: `calc((100% - ${theme.spacing(3)} * 2) / 3)`,
                },
              })}
              variant="outlined"
              elevation={0}
            >
              <Box
                component="iframe"
                key={title}
                title={title}
                sx={{ width: "100%", height: "100%" }}
                src={`${grafanaHost}${path}`}
                frameBorder="0"
              />
            </Paper>
          );
        })}
      </Box>
    </CollapsibleSection>
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
