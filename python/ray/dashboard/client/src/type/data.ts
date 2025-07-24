export type DatasetResponse = {
  datasets: DatasetMetrics[];
};

export type DatasetMetrics = DataMetrics & {
  dataset: string;
  job_id: string;
  operators: OperatorMetrics[];
  start_time: number;
  end_time: number | undefined;
};

export type OperatorMetrics = DataMetrics & {
  operator: string;
  name: string;
};

export type DataMetrics = {
  state: string;
  ray_data_current_bytes: {
    value: number;
    max: number;
  };
  ray_data_output_rows: {
    max: number;
  };
  ray_data_spilled_bytes: {
    max: number;
  };
  ray_data_cpu_usage_cores: {
    value: number;
    max: number;
  };
  ray_data_gpu_usage_cores: {
    value: number;
    max: number;
  };
  progress: number;
  total: number;
};
