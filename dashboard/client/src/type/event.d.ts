export type Event = {
  eventId: string;
  sourceType: SourceType;
  sourceHostname: string;
  hostName: string;
  sourcePid: number;
  pid: number;
  message: string;
  timestamp: number;
  severity: SeverityLevel;
  customFields: {
    [key: string]: any;
  };
};

export type EventRsp = {
  result: boolean;
  msg: string;
  data: {
    result: Event[];
  };
};

export enum SeverityLevel {
  INFO = "INFO",
  DEBUG = "DEBUG",
  WARNING = "WARNING",
  ERROR = "ERROR",
  TRACING = "TRACING",
} // to maintain and sync with event.proto
export enum SourceType {
  COMMON = "COMMON",
  CORE_WORKER = "CORE_WORKER",
  GCS = "GCS",
  RAYLET = "RAYLET",
  CLUSTER_LIFECYCLE = "CLUSTER_LIFECYCLE",
  AUTOSCALER = "AUTOSCALER",
  JOBS = "JOBS",
  SERVE = "SERVE",
} // to maintain and sync with event.proto

export type Align = "inherit" | "left" | "center" | "right" | "justify";

export type Filters = {
  sourceType: string[]; // TODO: Chao, multi-select severity level in filters button is a P1
  severityLevel: string[]; // TODO: Chao, multi-select severity level in filters button is a P1
  entityName: string | undefined;
  entityId: string | undefined;
};
