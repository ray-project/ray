import { GPUStats } from "./node";

export enum ActorEnum {
  DEPENDENCIES_UNREADY = "DEPENDENCIES_UNREADY",
  PENDING_CREATION = "PENDING_CREATION",
  ALIVE = "ALIVE",
  RESTARTING = "RESTARTING",
  DEAD = "DEAD",
}

export type Address = {
  rayletId: string;
  ipAddress: string;
  port: number;
  workerId: string;
};

export type Actor = {
  actorId: string;
  jobId: string;
  placementGroupId: string | null;
  state: ActorEnum | string; // PENDING, ALIVE, RECONSTRUCTING, DEAD
  pid: number | null;
  address: Address;
  name: string;
  numRestarts: string;
  actorClass: string;
  startTime: number | null;
  endTime: number | null;
  requiredResources: {
    [key: string]: number;
  };
  exitDetail: string;
  reprName: string;
};

export type ActorDetail = {
  workerId: string;
  numPendingTasks: number;
  taskQueueLength: number;
  numExecutedTasks: number;
  numInPlasma: number;
  numLocalObjects: number;
  numObjectRefsInScope: number;
  gpus?: GPUStats[]; // GPU stats fetched from node, 1 entry per GPU
  processStats: {
    cmdline: string[];
    cpuPercent: number;
    cpuTimes: {
      user: number;
      system: number;
      childrenUser: number;
      childrenUystem: number;
      iowait?: number;
    };
    createTime: number;
    memoryInfo: {
      rss: number; // aka “Resident Set Size”, this is the non-swapped physical memory a process has used. On UNIX it matches “top“‘s RES column). On Windows this is an alias for wset field and it matches “Mem Usage” column of taskmgr.exe.
      vms: number; // aka “Virtual Memory Size”, this is the total amount of virtual memory used by the process. On UNIX it matches “top“‘s VIRT column. On Windows this is an alias for pagefile field and it matches “Mem Usage” “VM Size” column of taskmgr.exe.
      pfaults: number; // number of page faults.
      pageins: number; // number of actual pageins.
      [key: string]: number;
    };
    pid: number | null;
  };
  mem?: number[]; // total memory, free memory, memory used ratio
} & Actor;
