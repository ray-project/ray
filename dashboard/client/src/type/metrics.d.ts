export type PrometheusQueryResp = {
  result: boolean;
  msg: string;
  data: {
    prometheusResp: {
      status: string;
      data: QueryRangeData;
    };
  };
};

export type QueryRangeData = {
  resultType: "matrix";
  result: QueryRangeMetric[];
};

export type QueryRangeMetric = {
  metric: {
    [tag: string]: string;
  };
  values: [number, string][];
};
