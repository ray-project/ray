import {
  Box,
  Button,
  InputAdornment,
  MenuItem,
  Paper,
  SxProps,
  TextField,
  Theme,
} from "@mui/material";
import React, { useContext, useEffect, useState } from "react";
import { BiRefresh, BiTime } from "react-icons/bi";
import { RiExternalLinkLine } from "react-icons/ri";
import { GlobalContext } from "../../App";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { ClassNameProps } from "../../common/props";
import { HelpInfo } from "../../components/Tooltip";
import {
  MetricConfig,
  REFRESH_VALUE,
  RefreshOptions,
  TIME_RANGE_TO_FROM_VALUE,
  TimeRangeOptions,
} from "../metrics";

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
  sx?: SxProps<Theme>;
};

export const ServeMetricsSection = ({
  className,
  metricsConfig,
  sx,
}: ServeMetricsSectionProps) => {
  const { grafanaHost, prometheusHealth, dashboardUids, dashboardDatasource } =
    useContext(GlobalContext);
  const grafanaServeDashboardUid = dashboardUids?.serve ?? "rayServeDashboard";
  const [refreshOption, setRefreshOption] = useState<RefreshOptions>(
    RefreshOptions.FIVE_SECONDS,
  );
  const [timeRangeOption, setTimeRangeOption] = useState<TimeRangeOptions>(
    TimeRangeOptions.FIVE_MINS,
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

  const fromParam = from !== null ? `&from=${from}` : "";
  const toParam = to !== null ? `&to=${to}` : "";
  const timeRangeParams = `${fromParam}${toParam}`;
  const refreshParams = refresh ? `&refresh=${refresh}` : "";

  return grafanaHost === undefined || !prometheusHealth ? null : (
    <CollapsibleSection
      className={className}
      sx={sx}
      title="Metrics"
      startExpanded
    >
      <div>
        <Box
          sx={{
            width: "100%",
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
            justifyContent: "flex-end",
            padding: 1,
            zIndex: 1,
            height: 36,
          }}
        >
          <Button
            href={`${grafanaHost}/d/${grafanaServeDashboardUid}?var-datasource=${dashboardDatasource}`}
            target="_blank"
            rel="noopener noreferrer"
            endIcon={<RiExternalLinkLine />}
          >
            View in Grafana
          </Button>
          <TextField
            sx={{ marginLeft: 2, width: 80 }}
            select
            size="small"
            value={refreshOption}
            onChange={({ target: { value } }) => {
              setRefreshOption(value as RefreshOptions);
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
            sx={{
              marginLeft: 2,
              width: 140,
            }}
            select
            size="small"
            value={timeRangeOption}
            onChange={({ target: { value } }) => {
              setTimeRangeOption(value as TimeRangeOptions);
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
        </Box>
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexWrap: "wrap",
            gap: 3,
            marginTop: 2,
          }}
        >
          {metricsConfig.map(({ title, pathParams }) => {
            const path =
              `/d-solo/${grafanaServeDashboardUid}?${pathParams}` +
              `${refreshParams}${timeRangeParams}&var-datasource=${dashboardDatasource}`;
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
      </div>
    </CollapsibleSection>
  );
};
