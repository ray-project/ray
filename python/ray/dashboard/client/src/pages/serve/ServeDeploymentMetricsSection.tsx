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

// NOTE: please keep the titles here in sync with dashboard/modules/metrics/dashboards/serve_deployment_dashboard_panels.py
const METRICS_CONFIG: MetricConfig[] = [
  {
    title: "QPS per replica",
    pathParams: "theme=light&panelId=2",
  },
  {
    title: "Error QPS per replica",
    pathParams: "&theme=light&panelId=3",
  },
  {
    title: "P90 latency per replica",
    pathParams: "&theme=light&panelId=5",
  },
];

type ServeDeploymentMetricsSectionProps = {
  deploymentName: string;
  replicaId: string;
  sx?: SxProps<Theme>;
} & ClassNameProps;

export const ServeReplicaMetricsSection = ({
  deploymentName,
  replicaId,
  className,
  sx,
}: ServeDeploymentMetricsSectionProps) => {
  const {
    grafanaHost,
    grafanaOrgId,
    prometheusHealth,
    dashboardUids,
    dashboardDatasource,
    currentTimeZone,
  } = useContext(GlobalContext);
  const grafanaServeDashboardUid =
    dashboardUids?.serveDeployment ?? "rayServeDashboard";

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

  const replicaButtonUrl = useViewServeDeploymentMetricsButtonUrl(
    deploymentName,
    replicaId,
  );

  return grafanaHost === undefined ||
    !prometheusHealth ||
    !replicaButtonUrl ? null : (
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
            href={replicaButtonUrl}
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
            sx={{ marginLeft: 2, width: 140 }}
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
          {METRICS_CONFIG.map(({ title, pathParams }) => {
            const path =
              `/d-solo/${grafanaServeDashboardUid}?orgId=${grafanaOrgId}&${pathParams}` +
              `${refreshParams}&timezone=${currentTimeZone}${timeRangeParams}&var-Deployment=${encodeURIComponent(
                deploymentName,
              )}&var-Replica=${encodeURIComponent(
                replicaId,
              )}&var-datasource=${dashboardDatasource}`;
            return (
              <Paper
                key={pathParams}
                sx={(theme) => ({
                  width: "100%",
                  height: 400,
                  overflow: "hidden",
                  [theme.breakpoints.up("md")]: {
                    // Calculate max width based on 1/3 of the total width minus gap between cards
                    width: `calc((100% - ${theme.spacing(3)} * 2) / 3)`,
                  },
                })}
                variant="outlined"
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

export const useViewServeDeploymentMetricsButtonUrl = (
  deploymentName: string,
  replicaId?: string,
) => {
  const {
    grafanaHost,
    grafanaOrgId,
    prometheusHealth,
    dashboardUids,
    dashboardDatasource,
  } = useContext(GlobalContext);
  const grafanaServeDashboardUid =
    dashboardUids?.serveDeployment ?? "rayServeDashboard";

  const replicaStr = replicaId
    ? `&var-Replica=${encodeURIComponent(replicaId)}`
    : "";

  return grafanaHost === undefined || !prometheusHealth
    ? null
    : `${grafanaHost}/d/${grafanaServeDashboardUid}?orgId=${grafanaOrgId}&var-Deployment=${encodeURIComponent(
        deploymentName,
      )}${replicaStr}&var-datasource=${dashboardDatasource}`;
};
