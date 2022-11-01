package io.ray.serve.config;

public class RayServeConfig {

  public static final String PROXY_CLASS = "ray.serve.proxy.class";

  public static final String PROXY_HTTP_PORT = "ray.serve.proxy.http.port";

  public static final String METRICS_ENABLED = "ray.serve.metrics.enabled";

  public static final String LONG_POOL_CLIENT_ENABLED = "ray.serve.long.poll.client.enabled";

  /** The polling interval of long poll thread. Unit: second. */
  public static final String LONG_POOL_CLIENT_INTERVAL = "ray.serve.long.poll.client.interval_s";

  /** The polling timeout of each long poll invoke. Unit: second. */
  public static final String LONG_POOL_CLIENT_TIMEOUT_S = "ray.serve.long.poll.client.timeout_s";
}
