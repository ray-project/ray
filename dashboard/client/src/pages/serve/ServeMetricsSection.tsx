import {
  Button,
  createStyles,
  makeStyles,
  MenuItem,
  Paper,
  TextField,
} from "@material-ui/core";
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
        width: `calc((100% - ${theme.spacing(3)}px * 2) / 3)`,
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
const METRICS_CONFIG: MetricConfig[] = [
  {
    title: "QPS per route",
    pathParams: "orgId=1&theme=light&panelId=7",
  },
  {
    title: "Error QPS per route",
    pathParams: "orgId=1&theme=light&panelId=8",
  },
  {
    title: "P90 latency per route",
    pathParams: "orgId=1&theme=light&panelId=15",
  },
];

type ServeMetricsSectionProps = ClassNameProps;

export const ServeMetricsSection = ({
  className,
}: ServeMetricsSectionProps) => {
  const classes = useStyles();
  const { grafanaHost, prometheusHealth, dashboardUids } =
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
        <Paper className={classes.topBar}>
          <Button
            href={`${grafanaHost}/d/${grafanaServeDashboardUid}`}
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
        <div className={classes.grafanaEmbedsContainer}>
          {METRICS_CONFIG.map(({ title, pathParams }) => {
            const path =
              `/d-solo/${grafanaServeDashboardUid}?${pathParams}` +
              `&refresh${timeRangeParams}`;
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
      </div>
    </CollapsibleSection>
  );
};
