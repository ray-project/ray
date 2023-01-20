import { get } from "../../service/requestHandlers";

const GRAFANA_HEALTHCHECK_URL = "/api/grafana_health";

type GrafanaHealthcheckRsp = {
  result: boolean;
  msg: string;
  data: {
    grafanaHost: string;
    sessionName: string;
  };
};

const fetchGrafanaHealthcheck = async () => {
  return await get<GrafanaHealthcheckRsp>(GRAFANA_HEALTHCHECK_URL);
};

export const getMetricsInfo = async () => {
  try {
    const resp = await fetchGrafanaHealthcheck();
    if (resp.data.result) {
      return {
        grafanaHost: resp.data.data.grafanaHost,
        sessionName: resp.data.data.sessionName,
      };
    }
  } catch (e) {}
  return { grafanaHost: undefined, sessionName: undefined };
};
