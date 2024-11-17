import { get } from "../../service/requestHandlers";

const GRAFANA_HEALTHCHECK_URL = "/api/grafana_health";
const PROMETHEUS_HEALTHCHECK_URL = "/api/prometheus_health";
const TIMEZONE_URL = "/timezone";

export type DashboardUids = {
  default: string;
  serve: string;
  serveDeployment: string;
  data: string;
};

type GrafanaHealthcheckRsp = {
  result: boolean;
  msg: string;
  data: {
    grafanaHost: string;
    sessionName: string;
    dashboardUids: DashboardUids;
    dashboardDatasource: string;
  };
};

type PrometheusHealthcheckRsp = {
  result: boolean;
  msg: string;
};

type TimezonekRsp = {
  offset: string | null;
  value: string | null;
};

const fetchGrafanaHealthcheck = async () => {
  return await get<GrafanaHealthcheckRsp>(GRAFANA_HEALTHCHECK_URL);
};

const fetchPrometheusHealthcheck = async () => {
  return await get<PrometheusHealthcheckRsp>(PROMETHEUS_HEALTHCHECK_URL);
};

type MetricsInfo = {
  grafanaHost?: string;
  sessionName?: string;
  prometheusHealth?: boolean;
  dashboardUids?: DashboardUids;
  dashboardDatasource?: string;
};

export const getMetricsInfo = async () => {
  const info: MetricsInfo = {
    grafanaHost: undefined,
    sessionName: undefined,
    prometheusHealth: undefined,
    dashboardUids: undefined,
    dashboardDatasource: undefined,
  };
  try {
    const resp = await fetchGrafanaHealthcheck();
    if (resp.data.result) {
      info.grafanaHost = resp.data.data.grafanaHost;
      info.sessionName = resp.data.data.sessionName;
      info.dashboardUids = resp.data.data.dashboardUids;
      info.dashboardDatasource = resp.data.data.dashboardDatasource;
    }
  } catch (e) {}
  try {
    const resp = await fetchPrometheusHealthcheck();
    if (resp.data.result) {
      info.prometheusHealth = resp.data.result;
    }
  } catch (e) {}

  return info;
};

type TimezoneInfo = {
  offset?: string | null;
  value?: string | null;
};
export const getTimeZoneInfo = async () => {
  const info: TimezoneInfo = {
    offset: undefined,
    value: undefined,
  };
  try {
    const resp = await get<TimezonekRsp>(TIMEZONE_URL);
    if (resp.data) {
      info.offset = resp.data.offset;
      info.value = resp.data.value;
    }
  } catch (e) {}
  return info;
};
