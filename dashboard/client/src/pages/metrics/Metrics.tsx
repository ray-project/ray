import {
  Button,
  createStyles,
  makeStyles,
  MenuItem,
  Paper,
  TextField,
} from "@material-ui/core";
import { Alert } from "@material-ui/lab";
import React, { useContext, useEffect, useState } from "react";

import { GlobalContext } from "../../App";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {},
    grafanaEmbedsContainer: {
      marginTop: theme.spacing(1),
      marginLeft: theme.spacing(1),
      display: "flex",
      flexDirection: "row",
      flexWrap: "wrap",
    },
    grafanaEmbed: {
      margin: theme.spacing(1),
    },
    topBar: {
      position: "sticky",
      width: "100%",
      display: "flex",
      flexDirection: "row",
      alignItems: "center",
      justifyContent: "flex-end",
      padding: theme.spacing(1),
    },
    timeRangeButton: {
      marginLeft: theme.spacing(2),
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

// NOTE: please keep the titles here in sync with grafana_dashboard_factory.py
const METRICS_CONFIG = [
  {
    title: "Scheduler Task State",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=26",
  },
  {
    title: "Active Tasks by Name",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=35",
  },
  {
    title: "Scheduler Actor State",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=33",
  },
  {
    title: "Active Actors by Name",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=36",
  },
  {
    title: "Scheduler CPUs (logical slots)",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=27",
  },
  {
    title: "Object Store Memory",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=29",
  },
  {
    title: "Scheduler GPUs (logical slots)",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=28",
  },
  {
    title: "Scheduler Placement Groups",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=40",
  },
  {
    title: "Node CPU (hardware utilization)",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=2",
  },
  {
    title: "Node GPU (hardware utilization)",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=8",
  },
  {
    title: "Node Disk",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=6",
  },
  {
    title: "Node Disk IO Speed",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=32",
  },
  {
    title: "Node Memory (heap + object store)",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=4",
  },
  {
    title: "Node Memory by Component",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=34",
  },
  {
    title: "Node CPU by Component",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=37",
  },
  {
    title: "Node GPU Memory (GRAM)",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=18",
  },
  {
    title: "Node Network",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=20",
  },
  {
    title: "Node Count",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=24",
  },
];

export const Metrics = () => {
  const classes = useStyles();
  const { grafanaHost, sessionName } = useContext(GlobalContext);

  const [timeRangeOption, setTimeRangeOption] = useState<TimeRangeOptions>(
    TimeRangeOptions.THIRTY_MINS,
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
    <div className={classes.root}>
      {grafanaHost === undefined ? (
        <Alert style={{ marginTop: 30 }} severity="warning">
          Grafana server not detected. Please make sure the grafana server is
          running and refresh this page. See:{" "}
          <a
            href="https://docs.ray.io/en/latest/ray-observability/ray-metrics.html"
            target="_blank"
            rel="noreferrer"
          >
            https://docs.ray.io/en/latest/ray-observability/ray-metrics.html
          </a>
          .
          <br />
          If you are hosting grafana on a separate machine or using a
          non-default port, please set the RAY_GRAFANA_HOST env var to point to
          your grafana server when launching ray.
        </Alert>
      ) : (
        <div>
          <Paper className={classes.topBar}>
            <Button
              href={grafanaHost}
              target="_blank"
              rel="noopener noreferrer"
            >
              View in Grafana
            </Button>
            <TextField
              className={classes.timeRangeButton}
              select
              size="small"
              variant="outlined"
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
          <div className={classes.grafanaEmbedsContainer}>
            {METRICS_CONFIG.map(({ title, path }) => (
              <iframe
                key={title}
                className={classes.grafanaEmbed}
                title={title}
                src={`${grafanaHost}${path}&refresh${timeRangeParams}&var-SessionName=${sessionName}`}
                width="450"
                height="400"
                frameBorder="0"
              />
            ))}
          </div>
        </div>
      )}
    </div>
  );
};
