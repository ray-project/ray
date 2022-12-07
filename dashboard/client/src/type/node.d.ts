import { Actor } from "./actor";
import { Raylet } from "./raylet";
import { Worker } from "./worker";

export type NodeDetail = {
  now: number;
  hostname: string;
  ip: string;
  cpu: number; // cpu usage
  cpus?: number[]; // Logic CPU Count, Physical CPU Count
  mem?: number[]; // total memory, free memory, memory used ratio
  gpus?: Array<GPUStats>; // GPU stats fetched from node, 1 entry per GPU
  bootTime: number; // start time
  loadAvg: number[][]; // recent 1，5，15 minitues system load，load per cpu http://man7.org/linux/man-pages/man3/getloadavg.3.html
  disk: {
    // disk used on root
    "/": {
      total: number;
      used: number;
      free: number;
      percent: number;
    };
    // disk used on tmp
    "/tmp": {
      total: number;
      used: number;
      free: number;
      percent: number;
    };
  };
  networkSpeed: number[]; // sent tps, received tps
  raylet: Raylet;
  logCounts: number;
  errorCounts: number;
  actors: { [id: string]: Actor };
  cmdline: string[];
  state: string;
  logUrl: string;
};

export type NodeListRsp = {
  data: {
    summary: NodeDetail[];
  };
  result: boolean;
  msg: string;
};

export type GPUProcessStats = {
  // Sub stat of GPU stats, this type represents the GPU
  // utilization of a single process of a single GPU.
  username: string;
  command: string;
  gpuMemoryUsage: number;
  pid: number;
};

export type GPUStats = {
  // This represents stats fetched from a node about a single GPU
  uuid: string;
  name: string;
  temperatureGpu: number;
  fanSpeed: number;
  utilizationGpu?: number;
  powerDraw: number;
  enforcedPowerLimit: number;
  memoryUsed: number;
  memoryTotal: number;
  processes?: GPUProcessStats[];
};

export type NodeDetailExtend = {
  workers: Worker[];
  raylet: Raylet;
  actors: {
    [actorId: string]: Actor;
  };
} & NodeDetail;

export type NodeDetailRsp = {
  data: {
    detail: NodeDetailExtend;
  };
  msg: string;
  result: boolean;
};
