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
  FIVE_MINS = "5 minutes",
  THIRTY_MINS = "30 minutes",
  ONE_HOUR = "1 hour",
  THREE_HOURS = "3 hours",
  SIX_HOURS = "6 hours",
  TWELVE_HOURS = "12 hours",
  ONE_DAY = "1 day",
  TWO_DAYS = "2 days",
  SEVEN_DAYS = "7 days",
}

const TIME_RANGE_DURATIONS_MS: Record<TimeRangeOptions, number> = {
  [TimeRangeOptions.FIVE_MINS]: 1000 * 60 * 5,
  [TimeRangeOptions.THIRTY_MINS]: 1000 * 60 * 30,
  [TimeRangeOptions.ONE_HOUR]: 1000 * 60 * 60 * 1,
  [TimeRangeOptions.THREE_HOURS]: 1000 * 60 * 60 * 3,
  [TimeRangeOptions.SIX_HOURS]: 1000 * 60 * 60 * 6,
  [TimeRangeOptions.TWELVE_HOURS]: 1000 * 60 * 60 * 12,
  [TimeRangeOptions.ONE_DAY]: 1000 * 60 * 60 * 24 * 1,
  [TimeRangeOptions.TWO_DAYS]: 1000 * 60 * 60 * 24 * 2,
  [TimeRangeOptions.SEVEN_DAYS]: 1000 * 60 * 60 * 24 * 7,
};

const METRICS_CONFIG = [
  {
    title: "Instance count",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=24",
  },
  {
    title: "Utilization percentage",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=10",
  },
  {
    title: "CPU usage",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=2",
  },
  {
    title: "Memory usage",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=4",
  },
  {
    title: "Disk usage",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=6",
  },
  {
    title: "Network speed",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=20",
  },
  {
    title: "Node GPU",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=8",
  },
  {
    title: "Node GPU memory",
    path: "/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=18",
  },
];

export const Metrics = () => {
  const classes = useStyles();
  const { grafanaHost } = useContext(GlobalContext);

  const [timeRangeOption, setTimeRangeOption] = useState<TimeRangeOptions>(
    TimeRangeOptions.ONE_HOUR,
  );
  const [[from, to], setTimeRange] = useState<[number | null, number | null]>([
    null,
    null,
  ]);
  useEffect(() => {
    const now = new Date().getTime();
    const duration = TIME_RANGE_DURATIONS_MS[timeRangeOption];
    const from = now - duration;
    setTimeRange([from, now]);
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
          <div className={classes.grafanaEmbedsContainer}>
            {METRICS_CONFIG.map(({ title, path }) => (
              <iframe
                key={title}
                className={classes.grafanaEmbed}
                title={title}
                src={`${grafanaHost}${path}${timeRangeParams}`}
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
