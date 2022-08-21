import {
  Button,
  createStyles,
  makeStyles,
  MenuItem,
  TextField,
  Typography,
} from "@material-ui/core";
import React, { useContext, useEffect, useState } from "react";

import { GlobalContext } from "../../App";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      height: "90vh",
    },
    grafanaEmbed: {
      display: "flex",
      width: "100%",
      height: "100%",
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
        <Typography color="error">
          Grafana healtcheck failed. Please make sure the grafana server is
          running and refresh the page.
        </Typography>
      ) : (
        <div>
          <Button href={grafanaHost} target="_blank" rel="noopener noreferrer">
            Go to Grafana
          </Button>
          <TextField
            select
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
          <br />
          <iframe
            title="Instance count"
            src={`${grafanaHost}/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=24${timeRangeParams}`}
            width="450"
            height="200"
            frameBorder="0"
          />
          <iframe
            title="Utilization percentage"
            src={`${grafanaHost}/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=10`}
            width="450"
            height="200"
            frameBorder="0"
          />
          <iframe
            title="CPU usage"
            src={`${grafanaHost}/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=2${timeRangeParams}`}
            width="450"
            height="200"
            frameBorder="0"
          />
          <iframe
            title="Memory usage"
            src={`${grafanaHost}/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=4${timeRangeParams}`}
            width="450"
            height="200"
            frameBorder="0"
          />
          <iframe
            title="Disk usage"
            src={`${grafanaHost}/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=6${timeRangeParams}`}
            width="450"
            height="200"
            frameBorder="0"
          />
          <iframe
            title="Network speed"
            src={`${grafanaHost}/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=20${timeRangeParams}`}
            width="450"
            height="200"
            frameBorder="0"
          />
          <iframe
            title="Node GPU"
            src={`${grafanaHost}/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=8${timeRangeParams}`}
            width="450"
            height="200"
            frameBorder="0"
          />
          <iframe
            title="Node GPU memory"
            src={`${grafanaHost}/d-solo/rayDefaultDashboard/default-dashboard?orgId=1&theme=light&panelId=18${timeRangeParams}`}
            width="450"
            height="200"
            frameBorder="0"
          />
        </div>
      )}
    </div>
  );
};
