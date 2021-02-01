export type CoreWorkerStats = {
  currentTaskFuncDesc: string;
  ipAddress: string;
  port: string;
  actorId: string;
  usedResources: { [key: string]: number };
  numExecutedTasks: number;
  workerId: string;
  actorTitle: string;
  jobId: string;
};

export type Worker = {
  createTime: number;
  cpuPercent: number;
  cmdline: string[];
  memoryInfo: {
    rss: number; // aka “Resident Set Size”, this is the non-swapped physical memory a process has used. On UNIX it matches “top“‘s RES column). On Windows this is an alias for wset field and it matches “Mem Usage” column of taskmgr.exe.
    vms: number; // aka “Virtual Memory Size”, this is the total amount of virtual memory used by the process. On UNIX it matches “top“‘s VIRT column. On Windows this is an alias for pagefile field and it matches “Mem Usage” “VM Size” column of taskmgr.exe.
    pfaults: number; // number of page faults.
    pageins: number; // number of actual pageins.
    [key: string]: number;
  };
  cpuTimes: {
    user: number;
    system: number;
    childrenUser: number;
    childrenUystem: number;
    iowait?: number;
  };
  pid: number;
  coreWorkerStats: CoreWorkerStats[];
  language: string;
  hostname: string;
  ip: hostname;
};
