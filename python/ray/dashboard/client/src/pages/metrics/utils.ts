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
    grafanaOrgId: string;
    grafanaClusterFilter: string | undefined;
    sessionName: string;
    dashboardUids: DashboardUids;
    dashboardDatasource: string;
  };
};

type PrometheusHealthcheckRsp = {
  result: boolean;
  msg: string;
};

type TimezoneRsp = {
  offset: string;
  value: string;
};

const fetchGrafanaHealthcheck = async () => {
  return await get<GrafanaHealthcheckRsp>(GRAFANA_HEALTHCHECK_URL);
};

const fetchPrometheusHealthcheck = async () => {
  return await get<PrometheusHealthcheckRsp>(PROMETHEUS_HEALTHCHECK_URL);
};

type MetricsInfo = {
  grafanaHost?: string;
  grafanaOrgId: string;
  grafanaClusterFilter: string | undefined;
  sessionName?: string;
  prometheusHealth?: boolean;
  dashboardUids?: DashboardUids;
  dashboardDatasource?: string;
};

export const getMetricsInfo = async () => {
  const info: MetricsInfo = {
    grafanaHost: undefined,
    grafanaOrgId: "1",
    grafanaClusterFilter: undefined,
    sessionName: undefined,
    prometheusHealth: undefined,
    dashboardUids: undefined,
    dashboardDatasource: undefined,
  };
  try {
    const resp = await fetchGrafanaHealthcheck();
    if (resp.data.result) {
      info.grafanaHost = resp.data.data.grafanaHost;
      info.grafanaOrgId = resp.data.data.grafanaOrgId;
      info.grafanaClusterFilter = resp.data.data.grafanaClusterFilter;
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

export type TimezoneInfo = {
  offset: string;
  value: string;
};

export const getTimeZoneInfo = async () => {
  try {
    const resp = await get<TimezoneRsp>(TIMEZONE_URL);
    if (resp.data) {
      return {
        offset: resp.data.offset,
        value: resp.data.value,
      };
    }
  } catch (e) {}
  return null;
};
