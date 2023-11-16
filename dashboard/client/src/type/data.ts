export type DatasetResponse = {
  datasets: DatasetMetrics[];
};

export type DatasetMetrics = DataMetrics & {
  dataset: string;
  operators: OperatorMetrics[];
  start_time: number;
  end_time: number | undefined;
};

export type OperatorMetrics = DataMetrics & {
  operator: string;
};

export type DataMetrics = {
  state: string;
  ray_data_current_bytes: {
    value: number;
    max: number;
  };
  ray_data_output_bytes: {
    max: number;
  };
  ray_data_spilled_bytes: {
    max: number;
  };
  progress: number;
  total: number;
};
