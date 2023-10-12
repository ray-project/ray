import { get } from "../../service/requestHandlers";
import { PrometheusQueryResp } from "../../type/metrics";

const GRAFANA_HEALTHCHECK_URL = "/api/grafana_health";
const PROMETHEUS_HEALTHCHECK_URL = "/api/prometheus_health";

export type DashboardUids = {
  default: string;
  serve: string;
  serveDeployment: string;
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
  data: {
    sessionName: string;
  };
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
      info.dashboardUids = resp.data.data.dashboardUids;
      info.dashboardDatasource = resp.data.data.dashboardDatasource;
    }
  } catch (e) {}
  try {
    const resp = await fetchPrometheusHealthcheck();
    if (resp.data.result) {
      info.prometheusHealth = resp.data.result;
      info.sessionName = resp.data.data.sessionName;
    }
  } catch (e) {}

  return info;
};

export const formatPrometheusResponse = (response: PrometheusQueryResp) => {
  return response.data.prometheusResp.data.result.map((line) => {
    return {
      name: JSON.stringify(line.metric),
      values: line.values.map(([time, val]) => ({
        time,
        val,
      })),
    };
  });
};

const COLORS = ["red", "blue", "green", "yellow", "purple", "orange", "pink"];

export const formatChartJsResponse = (response: PrometheusQueryResp) => {
  return {
    datasets: response.data.prometheusResp.data.result.map((line, idx) => {
      return {
        label: JSON.stringify(line.metric),
        data: line.values.map(([time, val]) => ({
          x: time * 1000,
          y: Number.parseFloat(val),
        })),
        fill: "stack",
        backgroundColor: COLORS[idx % COLORS.length],
      };
    }),
  };
};
