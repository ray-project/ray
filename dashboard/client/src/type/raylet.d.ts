export type ViewMeasures = {
  tags: string;
  int_value?: number;
  double_value?: number;
  distribution_min?: number;
  distribution_mean?: number;
  distribution_max?: number;
  distribution_count?: number;
  distribution_bucket_boundaries?: number[];
  distribution_bucket_counts?: number[];
};

export type ViewData = {
  viewName: string;
  measures: ViewMeasures[];
};

export type Raylet = {
  viewData: ViewData[];
  numWorkers: number;
  pid: number;
  nodeId: string;
  nodeManagerPort: number;
  brpcPort: pid;
  state: string;
  startTime: number;
  terminateTime: number;
};
