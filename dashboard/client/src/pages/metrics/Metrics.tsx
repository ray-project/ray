import {
  Button,
  createStyles,
  makeStyles,
  MenuItem,
  Paper,
  TextField,
} from "@material-ui/core";
import { Alert } from "@material-ui/lab";
import classNames from "classnames";
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
        width: `calc((100% - ${theme.spacing(3)}px * 2) / 3)`,
      },
    },
    grafanaEmbed: {
      width: "100%",
      height: "100%",
    },
    topBar: {
      position: "sticky",
      top: 0,
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
    topBarNewIA: {
      top: MAIN_NAV_HEIGHT,
    },
    timeRangeButton: {
      marginLeft: theme.spacing(2),
    },
    alert: {
      marginTop: 30,
    },
  }),
);

enum TimeRangeOptions {
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

const TIME_RANGE_TO_FROM_VALUE: Record<TimeRangeOptions, string> = {
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

type MetricConfig = {
  title: string;
  pathParams: string;
};

type MetricsSectionConfig = {
  title: string;
  contents: MetricConfig[];
};

// NOTE: please keep the titles here in sync with grafana_dashboard_factory.py
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

type MetricsProps = {
  newIA?: boolean;
};

export const Metrics = ({ newIA = false }: MetricsProps) => {
  const classes = useStyles();
  const {
    grafanaHost,
    sessionName,
    prometheusHealth,
    grafanaDefaultDashboardUid = "rayDefaultDashboard",
  } = useContext(GlobalContext);

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

  const fromParam = from !== null ? `&from=${from}` : "";
  const toParam = to !== null ? `&to=${to}` : "";
  const timeRangeParams = `${fromParam}${toParam}`;

  return (
    <div>
      <MainNavPageInfo
        pageInfo={{
          id: "metrics",
          title: "Metrics",
          path: "/new/metrics",
        }}
      />
      {grafanaHost === undefined || !prometheusHealth ? (
        <GrafanaNotRunningAlert className={classes.alert} />
      ) : (
        <div>
          <Paper
            className={classNames(classes.topBar, {
              [classes.topBarNewIA]: newIA,
            })}
          >
            <Button
              href={`${grafanaHost}/d/${grafanaDefaultDashboardUid}`}
              target="_blank"
              rel="noopener noreferrer"
              endIcon={<RiExternalLinkLine />}
            >
              View in Grafana
            </Button>
            <TextField
              className={classes.timeRangeButton}
              select
              size="small"
              style={{ width: 120 }}
              value={timeRangeOption}
              onChange={({ target: { value } }) => {
                setTimeRangeOption(value as TimeRangeOptions);
              }}
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
            {METRICS_CONFIG.map(({ title, contents }) => (
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
                      `/d-solo/${grafanaDefaultDashboardUid}?${pathParams}` +
                      `&refresh${timeRangeParams}&var-SessionName=${sessionName}`;
                    return (
                      <Paper
                        key={pathParams}
                        className={classes.chart}
                        elevation={1}
                        variant="outlined"
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
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export const GrafanaNotRunningAlert = ({ className }: ClassNameProps) => {
  const { grafanaHost, prometheusHealth } = useContext(GlobalContext);
  return grafanaHost === undefined || !prometheusHealth ? (
    <Alert className={className} severity="warning">
      Grafana or prometheus server not detected. Please make sure both services
      are running and refresh this page. See:{" "}
      <a
        href="https://docs.ray.io/en/latest/ray-observability/ray-metrics.html"
        target="_blank"
        rel="noreferrer"
      >
        https://docs.ray.io/en/latest/ray-observability/ray-metrics.html
      </a>
      .
      <br />
      If you are hosting grafana on a separate machine or using a non-default
      port, please set the RAY_GRAFANA_HOST env var to point to your grafana
      server when launching ray.
    </Alert>
  ) : null;
};
