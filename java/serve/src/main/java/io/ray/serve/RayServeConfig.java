package io.ray.serve;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RayServeConfig implements Serializable {

  private static final long serialVersionUID = 5367425336296141588L;

  public static final String PROXY_CLASS = "ray.serve.proxy.class";

  public static final String METRICS_ENABLED = "ray.serve.metrics.enabled";

  public static final String LONG_POOL_CLIENT_ENABLED = "ray.serve.long.poll.client.enabled";

  /** The polling interval of long poll thread. Unit: second. */
  public static final String LONG_POOL_CLIENT_INTERVAL = "ray.serve.long.poll.client.interval";

  private String name;

  private final Map<String, String> config = new HashMap<>();

  public String getName() {
    return name;
  }

  public RayServeConfig setName(String name) {
    this.name = name;
    return this;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public RayServeConfig setConfig(String key, String value) {
    this.config.put(key, value);
    return this;
  }

  public RayServeConfig setConfig(Map<String, String> config) {
    this.config.putAll(config);
    return this;
  }
}
