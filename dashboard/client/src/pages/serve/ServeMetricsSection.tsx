import { Box, Button, MenuItem, Paper, TextField } from "@mui/material";
import createStyles from "@mui/styles/createStyles";
import makeStyles from "@mui/styles/makeStyles";
import React, { useContext, useEffect, useState } from "react";
import { RiExternalLinkLine } from "react-icons/ri";
import { GlobalContext } from "../../App";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { ClassNameProps } from "../../common/props";
import {
  MetricConfig,
  TIME_RANGE_TO_FROM_VALUE,
  TimeRangeOptions,
} from "../metrics";

const useStyles = makeStyles((theme) =>
  createStyles({
    metricsRoot: { margin: theme.spacing(1) },
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
    topBar: {
      width: "100%",
      display: "flex",
      flexDirection: "row",
      alignItems: "center",
      justifyContent: "flex-end",
      padding: theme.spacing(1),
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

// NOTE: please keep the titles here in sync with dashboard/modules/metrics/dashboards/serve_dashboard_panels.py
export const APPS_METRICS_CONFIG: MetricConfig[] = [
  {
    title: "QPS per application",
    pathParams: "orgId=1&theme=light&panelId=7",
  },
  {
    title: "Error QPS per application",
    pathParams: "orgId=1&theme=light&panelId=8",
  },
  {
    title: "P90 latency per application",
    pathParams: "orgId=1&theme=light&panelId=15",
  },
];

// NOTE: please keep the titles here in sync with dashboard/modules/metrics/dashboards/serve_dashboard_panels.py
export const SERVE_SYSTEM_METRICS_CONFIG: MetricConfig[] = [
  {
    title: "Ongoing HTTP Requests",
    pathParams: "orgId=1&theme=light&panelId=20",
  },
  {
    title: "Ongoing gRPC Requests",
    pathParams: "orgId=1&theme=light&panelId=21",
  },
  {
    title: "Scheduling Tasks",
    pathParams: "orgId=1&theme=light&panelId=22",
  },
  {
    title: "Scheduling Tasks in Backoff",
    pathParams: "orgId=1&theme=light&panelId=23",
  },
  {
    title: "Controller Control Loop Duration",
    pathParams: "orgId=1&theme=light&panelId=24",
  },
  {
    title: "Number of Control Loops",
    pathParams: "orgId=1&theme=light&panelId=25",
  },
];

type ServeMetricsSectionProps = ClassNameProps & {
  metricsConfig: MetricConfig[];
};

export const ServeMetricsSection = ({
  className,
  metricsConfig,
}: ServeMetricsSectionProps) => {
  const classes = useStyles();
  const { grafanaHost, prometheusHealth, dashboardUids, dashboardDatasource } =
    useContext(GlobalContext);
  const grafanaServeDashboardUid = dashboardUids?.serve ?? "rayServeDashboard";

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

  return grafanaHost === undefined || !prometheusHealth ? null : (
    <CollapsibleSection className={className} title="Metrics" startExpanded>
      <div>
        <Box className={classes.topBar}>
          <Button
            href={`${grafanaHost}/d/${grafanaServeDashboardUid}?var-datasource=${dashboardDatasource}`}
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
        </Box>
        <div className={classes.grafanaEmbedsContainer}>
          {metricsConfig.map(({ title, pathParams }) => {
            const path =
              `/d-solo/${grafanaServeDashboardUid}?${pathParams}` +
              `&refresh${timeRangeParams}&var-datasource=${dashboardDatasource}`;
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
      </div>
    </CollapsibleSection>
  );
};
