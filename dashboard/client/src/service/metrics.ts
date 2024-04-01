import { PrometheusQueryResp } from "../type/metrics";
import { get } from "./requestHandlers";

export const queryPrometheus = (
  query: string,
  start: Date,
  end: Date,
  step_s: number,
) =>
  get<PrometheusQueryResp>("/api/prometheus_query_range", {
    params: {
      query,
      start: start.toISOString(),
      end: end.toISOString(),
      step: step_s,
    },
  });
