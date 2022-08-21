import { get } from "../../service/requestHandlers";

const GRAFANA_HEALTHCHECK_URL = "/api/grafana_health";

type GrafanaHealthcheckRsp = {
  result: boolean;
  msg: string;
  data: {
    grafanaHost: string;
  };
};

const fetchGrafanaHealthcheck = async () => {
  return await get<GrafanaHealthcheckRsp>(GRAFANA_HEALTHCHECK_URL);
};

export const getGrafanaHost = async () => {
  try {
    const resp = await fetchGrafanaHealthcheck();
    if (resp.data.result) {
      return resp.data.data.grafanaHost;
    }
  } catch (e) {}
  return undefined;
};
