export type ProcessUsage = {
  pid: number;
  memoryUsage: number;
};

export type AcceleratorStats = {
  uuid: string;
  index: number;
  name: string;
  utilization?: number;
  memoryUsed: number;
  memoryTotal: number;
  processes?: ProcessUsage[];
};

export type AcceleratorRsp = {
  data: {
    result: {
      columns: {
        label: string;
        helpInfo?: string;
      }[];
    }[];
  };
  result: boolean;
  msg: string;
};
