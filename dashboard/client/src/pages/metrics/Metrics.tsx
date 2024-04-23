import {
  Alert,
  AlertProps,
  Button,
  Link,
  Menu,
  MenuItem,
  Paper,
  TextField,
  Tooltip,
} from "@mui/material";
import createStyles from "@mui/styles/createStyles";
import makeStyles from "@mui/styles/makeStyles";
import React, { useContext, useEffect, useState } from "react";
import { RiExternalLinkLine } from "react-icons/ri";

import { GlobalContext } from "../../App";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { ClassNameProps } from "../../common/props";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { MAIN_NAV_HEIGHT } from "../layout/MainNavLayout";

const useStyles = makeStyles((theme) =>
  createStyles({
    metricsRoot: { margin: theme.spacing(1) },
    topBar: {
      position: "sticky",
      top: MAIN_NAV_HEIGHT,
      width: "100%",
      display: "flex",
      flexDirection: "row",
      alignItems: "center",
      justifyContent: "flex-end",
      padding: theme.spacing(1),
      boxShadow: "0px 1px 0px #D2DCE6",
      zIndex: 1,
      height: 36,
    },
    timeRangeButton: {
      marginLeft: theme.spacing(2),
    },
    alert: {
      marginTop: 30,
    },
  }),
);

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
        pathParams: "orgId=1&theme=light&panelId=26",
      },
      {
        title: "Active Tasks by Name",
        pathParams: "orgId=1&theme=light&panelId=35",
      },
      {
        title: "Scheduler Actor State",
        pathParams: "orgId=1&theme=light&panelId=33",
      },
      {
        title: "Active Actors by Name",
        pathParams: "orgId=1&theme=light&panelId=36",
      },
      {
        title: "Out of Memory Failures by Name",
        pathParams: "orgId=1&theme=light&panelId=44",
      },
    ],
  },
  {
    title: "Ray Resource Usage",
    contents: [
      {
        title: "Scheduler CPUs (logical slots)",
        pathParams: "orgId=1&theme=light&panelId=27",
      },
      {
        title: "Scheduler GPUs (logical slots)",
        pathParams: "orgId=1&theme=light&panelId=28",
      },
      {
        title: "Object Store Memory",
        pathParams: "orgId=1&theme=light&panelId=29",
      },
      {
        title: "Placement Groups",
        pathParams: "orgId=1&theme=light&panelId=40",
      },
    ],
  },
  {
    title: "Hardware Utilization",
    contents: [
      {
        title: "Node Count",
        pathParams: "orgId=1&theme=light&panelId=24",
      },
      {
        title: "Node CPU (hardware utilization)",
        pathParams: "orgId=1&theme=light&panelId=2",
      },
      {
        title: "Node Memory (heap + object store)",
        pathParams: "orgId=1&theme=light&panelId=4",
      },
      {
        title: "Node GPU (hardware utilization)",
        pathParams: "orgId=1&theme=light&panelId=8",
      },
      {
        title: "Node GPU Memory (GRAM)",
        pathParams: "orgId=1&theme=light&panelId=18",
      },
      {
        title: "Node Disk",
        pathParams: "orgId=1&theme=light&panelId=6",
      },
      {
        title: "Node Disk IO Speed",
        pathParams: "orgId=1&theme=light&panelId=32",
      },
      {
        title: "Node Network",
        pathParams: "orgId=1&theme=light&panelId=20",
      },
      {
        title: "Node CPU by Component",
        pathParams: "orgId=1&theme=light&panelId=37",
      },
      {
        title: "Node Memory by Component",
        pathParams: "orgId=1&theme=light&panelId=34",
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
        pathParams: "orgId=1&theme=light&panelId=1",
      },
      {
        title: "Bytes Allocated",
        pathParams: "orgId=1&theme=light&panelId=2",
      },
      {
        title: "Bytes Freed",
        pathParams: "orgId=1&theme=light&panelId=3",
      },
      {
        title: "Object Store Memory",
        pathParams: "orgId=1&theme=light&panelId=4",
      },
      {
        title: "CPUs (logical slots)",
        pathParams: "orgId=1&theme=light&panelId=5",
      },
      {
        title: "GPUs (logical slots)",
        pathParams: "orgId=1&theme=light&panelId=6",
      },
      {
        title: "Bytes Outputted",
        pathParams: "orgId=1&theme=light&panelId=7",
      },
      {
        title: "Rows Outputted",
        pathParams: "orgId=1&theme=light&panelId=11",
      },
    ],
  },
  {
    title: "Ray Data Metrics (Inputs)",
    contents: [
      {
        title: "Input Blocks Received by Operator",
        pathParams: "orgId=1&theme=light&panelId=17",
      },
      {
        title: "Input Blocks Processed by Tasks",
        pathParams: "orgId=1&theme=light&panelId=19",
      },
      {
        title: "Input Bytes Processed by Tasks",
        pathParams: "orgId=1&theme=light&panelId=20",
      },
      {
        title: "Input Bytes Submitted to Tasks",
        pathParams: "orgId=1&theme=light&panelId=21",
      },
    ],
  },
  {
    title: "Ray Data Metrics (Outputs)",
    contents: [
      {
        title: "Blocks Generated by Tasks",
        pathParams: "orgId=1&theme=light&panelId=22",
      },
      {
        title: "Bytes Generated by Tasks",
        pathParams: "orgId=1&theme=light&panelId=23",
      },
      {
        title: "Rows Generated by Tasks",
        pathParams: "orgId=1&theme=light&panelId=24",
      },
      {
        title: "Output Blocks Taken by Downstream Operators",
        pathParams: "orgId=1&theme=light&panelId=25",
      },
      {
        title: "Output Bytes Taken by Downstream Operators",
        pathParams: "orgId=1&theme=light&panelId=26",
      },
    ],
  },
  {
    title: "Ray Data Metrics (Tasks)",
    contents: [
      {
        title: "Submitted Tasks",
        pathParams: "orgId=1&theme=light&panelId=29",
      },
      {
        title: "Running Tasks",
        pathParams: "orgId=1&theme=light&panelId=30",
      },
      {
        title: "Tasks with output blocks",
        pathParams: "orgId=1&theme=light&panelId=31",
      },
      {
        title: "Finished Tasks",
        pathParams: "orgId=1&theme=light&panelId=32",
      },
      {
        title: "Failed Tasks",
        pathParams: "orgId=1&theme=light&panelId=33",
      },
      {
        title: "Block Generation Time",
        pathParams: "orgId=1&theme=light&panelId=8",
      },
      {
        title: "Task Submission Backpressure Time",
        pathParams: "orgId=1&theme=light&panelId=37",
      },
    ],
  },
  {
    title: "Ray Data Metrics (Object Store Memory)",
    contents: [
      {
        title: "Operator Internal Inqueue Size (Blocks)",
        pathParams: "orgId=1&theme=light&panelId=13",
      },
      {
        title: "Operator Internal Inqueue Size (Bytes)",
        pathParams: "orgId=1&theme=light&panelId=14",
      },
      {
        title: "Operator Internal Outqueue Size (Blocks)",
        pathParams: "orgId=1&theme=light&panelId=15",
      },
      {
        title: "Operator Internal Outqueue Size (Bytes)",
        pathParams: "orgId=1&theme=light&panelId=16",
      },
      {
        title: "Size of Blocks used in Pending Tasks (Bytes)",
        pathParams: "orgId=1&theme=light&panelId=34",
      },
      {
        title: "Freed Memory in Object Store (Bytes)",
        pathParams: "orgId=1&theme=light&panelId=35",
      },
      {
        title: "Spilled Memory in Object Store (Bytes)",
        pathParams: "orgId=1&theme=light&panelId=36",
      },
    ],
  },
  {
    title: "Ray Data Metrics (Iteration)",
    contents: [
      {
        title: "Iteration Initialization Time",
        pathParams: "orgId=1&theme=light&panelId=12",
      },
      {
        title: "Iteration Blocked Time",
        pathParams: "orgId=1&theme=light&panelId=9",
      },
      {
        title: "Iteration User Time",
        pathParams: "orgId=1&theme=light&panelId=10",
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
  const classes = useStyles();
  const { grafanaHost, prometheusHealth, dashboardUids, dashboardDatasource } =
    useContext(GlobalContext);

  const grafanaDefaultDashboardUid =
    dashboardUids?.default ?? "rayDefaultDashboard";

  const grafanaDefaultDatasource = dashboardDatasource ?? "Prometheus";

  const [timeRangeOption, setTimeRangeOption] = useState<TimeRangeOptions>(
    TimeRangeOptions.FIVE_MINS,
  );
  const [[from, to], setTimeRange] = useState<[string | null, string | null]>([
    null,
    null,
  ]);
  useEffect(() => {
    const from = TIME_RANGE_TO_FROM_VALUE[timeRangeOption];
    setTimeRange([from, "now"]);
  }, [timeRangeOption]);

  const [viewInGrafanaMenuRef, setViewInGrafanaMenuRef] =
    useState<HTMLButtonElement | null>(null);

  const fromParam = from !== null ? `&from=${from}` : "";
  const toParam = to !== null ? `&to=${to}` : "";
  const timeRangeParams = `${fromParam}${toParam}`;

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
        <GrafanaNotRunningAlert className={classes.alert} />
      ) : (
        <div>
          <Paper className={classes.topBar}>
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
                  href={`${grafanaHost}/d/${grafanaDefaultDashboardUid}/?var-datasource=${grafanaDefaultDatasource}`}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Core Dashboard
                </MenuItem>
                {dashboardUids?.["data"] && (
                  <Tooltip title="The Ray Data dashboard has a dropdown to filter the data metrics by Dataset ID">
                    <MenuItem
                      component="a"
                      href={`${grafanaHost}/d/${dashboardUids["data"]}/?var-datasource=${grafanaDefaultDatasource}`}
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
              className={classes.timeRangeButton}
              select
              size="small"
              style={{ width: 120 }}
              value={timeRangeOption}
              onChange={({ target: { value } }) => {
                setTimeRangeOption(value as TimeRangeOptions);
              }}
              variant="standard"
            >
              {Object.entries(TimeRangeOptions).map(([key, value]) => (
                <MenuItem key={key} value={value}>
                  {value}
                </MenuItem>
              ))}
            </TextField>
          </Paper>
          <Alert severity="info">
            Tip: You can click on the legend to focus on a specific line in the
            time-series graph. You can use control/cmd + click to filter out a
            line in the time-series graph.
          </Alert>
          <div className={classes.metricsRoot}>
            {METRICS_CONFIG.map((config) => (
              <MetricsSection
                key={config.title}
                metricConfig={config}
                timeRangeParams={timeRangeParams}
                dashboardUid={grafanaDefaultDashboardUid}
                dashboardDatasource={grafanaDefaultDatasource}
              />
            ))}
            {dashboardUids?.["data"] &&
              DATA_METRICS_CONFIG.map((config) => (
                <MetricsSection
                  key={config.title}
                  metricConfig={config}
                  timeRangeParams={timeRangeParams}
                  dashboardUid={dashboardUids["data"]}
                  dashboardDatasource={grafanaDefaultDatasource}
                />
              ))}
          </div>
        </div>
      )}
    </div>
  );
};

const useMetricsSectionStyles = makeStyles((theme) =>
  createStyles({
    metricsSection: {
      marginTop: theme.spacing(3),
    },
    grafanaEmbedsContainer: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "wrap",
      gap: theme.spacing(3),
      marginTop: theme.spacing(2),
    },
    chart: {
      width: "100%",
      height: 400,
      overflow: "hidden",
      [theme.breakpoints.up("md")]: {
        // Calculate max width based on 1/3 of the total width minus padding between cards
        width: `calc((100% - ${theme.spacing(3)} * 2) / 3)`,
      },
    },
    grafanaEmbed: {
      width: "100%",
      height: "100%",
    },
  }),
);

type MetricsSectionProps = {
  metricConfig: MetricsSectionConfig;
  timeRangeParams: string;
  dashboardUid: string;
  dashboardDatasource: string;
};

const MetricsSection = ({
  metricConfig: { title, contents },
  timeRangeParams,
  dashboardUid,
  dashboardDatasource,
}: MetricsSectionProps) => {
  const { grafanaHost, sessionName } = useContext(GlobalContext);

  const classes = useMetricsSectionStyles();

  return (
    <CollapsibleSection
      key={title}
      title={title}
      startExpanded
      className={classes.metricsSection}
      keepRendered
    >
      <div className={classes.grafanaEmbedsContainer}>
        {contents.map(({ title, pathParams }) => {
          const path =
            `/d-solo/${dashboardUid}?${pathParams}` +
            `&refresh${timeRangeParams}&var-SessionName=${sessionName}&var-datasource=${dashboardDatasource}`;
          return (
            <Paper
              key={pathParams}
              className={classes.chart}
              variant="outlined"
              elevation={0}
            >
              <iframe
                key={title}
                title={title}
                className={classes.grafanaEmbed}
                src={`${grafanaHost}${path}`}
                frameBorder="0"
              />
            </Paper>
          );
        })}
      </div>
    </CollapsibleSection>
  );
};

const useGrafanaNotRunningAlertStyles = makeStyles((theme) =>
  createStyles({
    heading: {
      fontWeight: 500,
    },
  }),
);

export type GrafanaNotRunningAlertProps = {
  severity?: AlertProps["severity"];
} & ClassNameProps;

export const GrafanaNotRunningAlert = ({
  className,
  severity = "warning",
}: GrafanaNotRunningAlertProps) => {
  const classes = useGrafanaNotRunningAlertStyles();

  const { grafanaHost, prometheusHealth } = useContext(GlobalContext);
  return grafanaHost === undefined || !prometheusHealth ? (
    <Alert className={className} severity={severity}>
      <span className={classes.heading}>
        Set up Prometheus and Grafana for better Ray Dashboard experience
      </span>
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
