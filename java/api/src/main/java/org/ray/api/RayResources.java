package org.ray.api;

import org.apache.log4j.lf5.util.ResourceUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO(qwang): doc
 */
public final class RayResources {
  public static final String CPU_LITERAL = "CPU";
  public static final String GPU_LITERAL = "GPU";

  /**
   * TODO(qwang): doc
   */
  private Map<String, Double> resources;

  RayResources() {
    resources = new HashMap<>();
  }

  public void add(String name, Double value) {
    resources.put(name, value);
  }

  public Double get(String name) {
    if (!resources.containsKey(name)) {
      throw new IllegalArgumentException(String.format("Resource %s don't exist.", name));
    }

    return resources.get(name);
  }

  public Map<String, Double> getAll() {
    if (!resources.containsKey(CPU_LITERAL)) {
      resources.put(CPU_LITERAL, 0.0);
    }

    return resources;
  }

  @Override
  public String toString() {
    return "TODO";
  }

}
