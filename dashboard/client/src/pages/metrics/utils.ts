import { get } from "../../service/requestHandlers";

const GRAFANA_HEALTHCHECK_URL = "/api/grafana_health";
const PROMETHEUS_HEALTHCHECK_URL = "/api/prometheus_health";

type GrafanaHealthcheckRsp = {
  result: boolean;
  msg: string;
  data: {
    grafanaHost: string;
    sessionName: string;
    grafanaDefaultDashboardUid: string;
  };
};

type PrometheusHealthcheckRsp = {
  result: boolean;
  msg: string;
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
  grafanaDefaultDashboardUid?: string;
};

export const getMetricsInfo = async () => {
  const info: MetricsInfo = {
    grafanaHost: undefined,
    sessionName: undefined,
    prometheusHealth: undefined,
    grafanaDefaultDashboardUid: undefined,
  };
  try {
    const resp = await fetchGrafanaHealthcheck();
    if (resp.data.result) {
      info.grafanaHost = resp.data.data.grafanaHost;
      info.sessionName = resp.data.data.sessionName;
      info.grafanaDefaultDashboardUid =
        resp.data.data.grafanaDefaultDashboardUid;
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
