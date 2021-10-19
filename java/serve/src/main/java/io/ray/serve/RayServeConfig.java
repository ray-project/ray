package io.ray.serve;

import java.io.Serializable;
import java.util.Map;

public class RayServeConfig implements Serializable {

  private static final long serialVersionUID = 5367425336296141588L;

  public static final String PROXY_CLASS = "ray.serve.proxy.class";

  public static final String METRICS_ENABLED = "ray.serve.metrics.enabled";

  private String name;

  private Map<String, String> config;

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

  public RayServeConfig setConfig(Map<String, String> config) {
    this.config = config;
    return this;
  }
}
