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
