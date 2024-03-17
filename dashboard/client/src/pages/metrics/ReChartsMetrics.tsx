import {
  Button,
  createStyles,
  makeStyles,
  MenuItem,
  Paper,
  TextField,
  Typography,
} from "@material-ui/core";
import { Alert, AlertProps } from "@material-ui/lab";
import { addSeconds, subMinutes } from "date-fns";
import dayjs from "dayjs";
import React, { useContext, useEffect, useState } from "react";
import { Chart } from "react-chartjs-2";
import { RiExternalLinkLine } from "react-icons/ri";

import {
  Area,
  AreaChart,
  CartesianGrid,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import useSWR from "swr";
import { GlobalContext } from "../../App";
import { ClassNameProps } from "../../common/props";
import Loading from "../../components/Loading";
import { queryPrometheus } from "../../service/metrics";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { MAIN_NAV_HEIGHT } from "../layout/MainNavLayout";
import { formatChartJsResponse, formatPrometheusResponse } from "./utils";

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

type GraphConfig = {
  query: (sessionName: string) => string;
};

const CHARTS: GraphConfig[] = [
  {
    query: (sessionName: string) =>
      `sum(max_over_time(ray_tasks{IsRetry="0",State=~"FINISHED|FAILED",SessionName=~"${sessionName}",}[14d])) by (State) or clamp_min(sum(ray_tasks{IsRetry="0",State!~"FINISHED|FAILED",SessionName=~"${sessionName}",}) by (State), 0)`,
  },
];

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

export const TIME_RANGE_TO_MINUTES: Record<TimeRangeOptions, number> = {
  [TimeRangeOptions.FIVE_MINS]: 5,
  [TimeRangeOptions.THIRTY_MINS]: 30,
  [TimeRangeOptions.ONE_HOUR]: 60,
  [TimeRangeOptions.THREE_HOURS]: 60 * 3,
  [TimeRangeOptions.SIX_HOURS]: 60 * 6,
  [TimeRangeOptions.TWELVE_HOURS]: 60 * 12,
  [TimeRangeOptions.ONE_DAY]: 60 * 24,
  [TimeRangeOptions.TWO_DAYS]: 60 * 24 * 2,
  [TimeRangeOptions.SEVEN_DAYS]: 60 * 24 * 7,
};

export const ReChartsMetrics = () => {
  const classes = useStyles();
  const { prometheusHealth } = useContext(GlobalContext);

  const [timeRangeOption, setTimeRangeOption] = useState<TimeRangeOptions>(
    TimeRangeOptions.FIVE_MINS,
  );
  const [[from, to, step_s], setTimeRange] = useState<
    [Date | null, Date | null, number]
  >([null, null, 100]);
  useEffect(() => {
    const minutes = TIME_RANGE_TO_MINUTES[timeRangeOption];
    const now = new Date();
    const from = subMinutes(now, minutes);
    const step_s = (now.getTime() - from.getTime()) / 1000 / 40;

    setTimeRange([from, now, step_s]);
  }, [timeRangeOption]);

  return (
    <div>
      <MainNavPageInfo
        pageInfo={{
          id: "metrics",
          title: "Metrics",
          path: "/metrics",
        }}
      />
      {!prometheusHealth ? (
        <GrafanaNotRunningAlert className={classes.alert} />
      ) : (
        <div>
          <Paper className={classes.topBar}>
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
          <div className={classes.metricsRoot}>
            {CHARTS.map((config, idx) => (
              <ChartJSGraph
                key={idx}
                config={config}
                from={from}
                to={to}
                step_s={step_s}
              />
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

const Graph = ({
  config,
  from,
  to,
  step_s,
}: {
  config: GraphConfig;
  from: Date | null;
  to: Date | null;
  step_s: number;
}) => {
  const { sessionName } = useContext(GlobalContext);

  const query = sessionName ? config.query(sessionName) : undefined;
  const { data, error, isLoading } = useSWR(
    query && from && to ? [query, from, to] : null,
    async ([query, from, to]) => {
      const resp = await queryPrometheus(query, from, to, step_s);
      if (resp.status !== 200) {
        throw new Error("Failed to query Prometheus: " + resp.statusText);
      }
      return formatPrometheusResponse(resp.data);
    },
  );

  if (error) {
    return <Typography color="error">{error.toString()}</Typography>;
  }

  if (isLoading || !data) {
    return <Loading loading />;
  }

  return (
    <AreaChart width={730} height={250} data={data?.[0].values}>
      <defs>
        <linearGradient id="colorUv" x1="0" y1="0" x2="0" y2="1">
          <stop offset="5%" stopColor="#8884d8" stopOpacity={0.8} />
          <stop offset="95%" stopColor="#8884d8" stopOpacity={0} />
        </linearGradient>
        <linearGradient id="colorPv" x1="0" y1="0" x2="0" y2="1">
          <stop offset="5%" stopColor="#82ca9d" stopOpacity={0.8} />
          <stop offset="95%" stopColor="#82ca9d" stopOpacity={0} />
        </linearGradient>
      </defs>
      <XAxis
        name="foo"
        dataKey="time"
        tickFormatter={(timeStr) => dayjs(timeStr).format("HH:mm:ss")}
        allowDuplicatedCategory={false}
      />
      <YAxis />
      <CartesianGrid strokeDasharray="3 3" />
      <Tooltip />
      {data.map(({ name, values }) => (
        <Area
          key={name}
          data={values}
          name={name}
          type="monotone"
          dataKey="val"
          stroke="#8884d8"
          fillOpacity={1}
          fill="url(#colorUv)"
          stackId="1"
          // connectNulls
        />
      ))}
    </AreaChart>
  );
};

const useGrafanaNotRunningAlertStyles = makeStyles((theme) =>
  createStyles({
    heading: {
      fontWeight: 500,
    },
  }),
);

type GrafanaNotRunningAlertProps = {
  severity?: AlertProps["severity"];
} & ClassNameProps;

const GrafanaNotRunningAlert = ({
  className,
  severity = "warning",
}: GrafanaNotRunningAlertProps) => {
  const classes = useGrafanaNotRunningAlertStyles();

  const { prometheusHealth } = useContext(GlobalContext);
  return !prometheusHealth ? (
    <Alert className={className} severity={severity}>
      <span className={classes.heading}>
        Set up Prometheus for better Ray Dashboard experience
      </span>
      <br />
      <br />
      Time-series charts are hidden because either Prometheus server is not
      detected. Follow{" "}
      <a
        href="https://docs.ray.io/en/latest/cluster/metrics.html"
        target="_blank"
        rel="noreferrer"
      >
        these instructions
      </a>{" "}
      to set them up and refresh this page. .
    </Alert>
  ) : null;
};

const ChartJSGraph = ({
  config,
  from,
  to,
  step_s,
}: {
  config: GraphConfig;
  from: Date | null;
  to: Date | null;
  step_s: number;
}) => {
  const { sessionName } = useContext(GlobalContext);

  const query = sessionName ? config.query(sessionName) : undefined;
  const { data, error, isLoading } = useSWR(
    query && from && to ? [query, from, to] : null,
    async ([query, from, to]) => {
      const resp = await queryPrometheus(query, from, to, step_s);
      if (resp.status !== 200) {
        throw new Error("Failed to query Prometheus: " + resp.statusText);
      }
      return formatChartJsResponse(resp.data);
    },
  );

  if (error) {
    return <Typography color="error">{error.toString()}</Typography>;
  }

  if (isLoading || !data) {
    return <Loading loading />;
  }
  console.log("data", data);

  return (
    <Chart
      type="line"
      data={data}
      options={{
        scales: {
          x: {
            min: from?.getTime(),
            max: to?.getTime(),
            type: "timeseries",
          },
          y: {
            stacked: true,
            min: 0,
          },
        },
        // plugins: {
        //   filler: {
        //     propagate: true,
        //   },
        // },
      }}
    />
  );
};
