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
  ;

  public static final String TAG_HANDLE = "handle";

  public static final String TAG_ENDPOINT = "endpoint";

  public static final String TAG_DEPLOYMENT = "deployment";

  private static final boolean isMetricsEnabled =
      Ray.isInitialized() && !Ray.getRuntimeContext().isSingleProcess();

  private String name;

  private String description;

  private RayServeMetrics(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public static void execute(Runnable runnable) {
    if (isMetricsEnabled) {
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
