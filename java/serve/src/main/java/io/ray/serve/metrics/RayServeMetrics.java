package io.ray.serve.metrics;

import io.ray.api.Ray;

public enum RayServeMetrics {
  SERVE_HANDLE_REQUEST_COUNTER(
      "serve_handle_request_counter",
      "The number of handle.remote() calls that have been made on this handle."),

  SERVE_NUM_ROUTER_REQUESTS(
      "serve_num_router_requests", "The number of requests processed by the router."),

  SERVE_ROUTER_NUM_QUEUED_REQUESTS(
      "serve_router_num_queued_requests",
      "The current number of queries to this deployment waiting to be assigned to a replica."),

  // Deprecated: Will be removed in Ray 3.0.
  SERVE_DEPLOYMENT_QUEUED_QUERIES_DEPRECATED(
      "serve_deployment_queued_queries",
      "(Deprecated, use serve_router_num_queued_requests) The current number of queries to this deployment waiting to be assigned to a replica."),

  SERVE_DEPLOYMENT_REQUEST_COUNTER(
      "serve_deployment_request_counter",
      "The number of queries that have been processed in this replica."),

  SERVE_DEPLOYMENT_ERROR_COUNTER(
      "serve_deployment_error_counter",
      "The number of exceptions that have occurred in this replica."),

  SERVE_DEPLOYMENT_REPLICA_STARTS(
      "serve_deployment_replica_starts",
      "The number of times this replica has been restarted due to failure."),

  SERVE_DEPLOYMENT_PROCESSING_LATENCY_MS(
      "serve_deployment_processing_latency_ms", "The latency for queries to be processed."),

  SERVE_REPLICA_NUM_ONGOING_REQUESTS(
      "serve_replica_num_ongoing_requests", "The current number of queries being processed."),

  // Deprecated: Will be removed in Ray 3.0.
  SERVE_REPLICA_PROCESSING_QUERIES_DEPRECATED(
      "serve_replica_processing_queries",
      "(Deprecated, use serve_replica_num_ongoing_requests) The current number of queries being processed."),
  ;

  public static final String TAG_HANDLE = "handle";

  public static final String TAG_ENDPOINT = "endpoint";

  public static final String TAG_DEPLOYMENT = "deployment";

  public static final String TAG_ROUTE = "route";

  public static final String TAG_REPLICA = "replica";

  public static final String TAG_APPLICATION = "application";

  private static final boolean canBeUsed =
      Ray.isInitialized() && !Ray.getRuntimeContext().isLocalMode();

  private static volatile boolean enabled = true;

  private String name;

  private String description;

  private RayServeMetrics(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public static void execute(Runnable runnable) {
    if (!enabled || !canBeUsed) {
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

  public static void enable() {
    enabled = true;
  }

  public static void disable() {
    enabled = false;
  }
}
