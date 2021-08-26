import { Actor } from "./actor";
import { Worker } from "./worker";

export type Job = {
  jobId: string;
  name: string;
  owner: string;
  language: string;
  driverEntry: string;
  state: string;
  timestamp: number;
  namespaceId: string;
  driverPid: number;
  driverIpAddress: string;
  isDead: boolean;
};

export type PythonDependenciey = string;

export type JavaDependency = {
  name: string;
  version: string;
  md5: string;
  url: string;
};

export type JobInfo = {
  url: string;
  driverArgs: string;
  customConfig: {
    [k: string]: string;
  };
  jvmOptions: string;
  dependencies: {
    python: PythonDependenciey[];
    java: JavaDependency[];
  };
  driverStarted: boolean;
  submitTime: string;
  startTime: null | string | number;
  endTime: null | string | number;
  driverIpAddress: string;
  driverHostname: string;
  driverPid: number;
  eventUrl: string;
  failErrorMessage: string;
  driverCmdline: string;
} & Job;

export type JobDetail = {
  jobInfo: JobInfo;
  jobActors: { [id: string]: Actor };
  jobWorkers: Worker[];
};

export type JobDetailRsp = {
  data: {
    detail: JobDetail;
  };
  msg: string;
  result: boolean;
};

export type JobListRsp = {
  data: {
    summary: Job[];
  };
  msg: string;
  result: boolean;
};
