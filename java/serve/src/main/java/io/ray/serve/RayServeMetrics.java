package io.ray.serve;

import io.ray.api.Ray;

public enum RayServeMetrics {
  SERVE_HANDLE_REQUEST_COUNTER(
      "serve_handle_request_counter",
      "The number of handle.remote() calls that have been made on this handle."),

  SERVE_NUM_ROUTER_REQUESTS(
      "serve_num_router_requests", "The number of requests processed by the router."),

  SERVE_DEPLOYMENT_QUEUED_QUERIES(
      "serve_deployment_queued_queries",
      "The current number of queries to this deployment waiting to be assigned to a replica."),

  SERVE_BACKEND_REQUEST_COUNTER(
      "serve_backend_request_counter",
      "The number of queries that have been processed in this replica."),

  SERVE_BACKEND_ERROR_COUNTER(
      "serve_backend_error_counter",
      "The number of exceptions that have occurred in this replica."),

  SERVE_BACKEND_REPLICA_STARTS(
      "serve_backend_replica_starts",
      "The number of times this replica has been restarted due to failure."),

  SERVE_BACKEND_PROCESSING_LATENCY_MS(
      "serve_backend_processing_latency_ms", "The latency for queries to be processed."),

  SERVE_REPLICA_PROCESSING_QUERIES(
      "serve_replica_processing_queries", "The current number of queries being processed."),
  ;

  public static final String TAG_HANDLE = "handle";

  public static final String TAG_ENDPOINT = "endpoint";

  public static final String TAG_DEPLOYMENT = "deployment";

  public static final String TAG_ROUTE = "route";

  public static final String TAG_BACKEND = "backend";

  public static final String TAG_REPLICA = "replica";

  private static final boolean isMetricsEnabled =
      Ray.isInitialized() && !Ray.getRuntimeContext().isSingleProcess();

  private String name;

  private String description;

  private RayServeMetrics(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public static void execute(Runnable runnable) {
    if (!isMetricsEnabled) {
      return;
    }
    runnable.run();
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }
}
