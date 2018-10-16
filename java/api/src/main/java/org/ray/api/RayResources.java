package org.ray.api;

import java.util.HashMap;
import java.util.Map;

public final class RayResources {
  public static final String CPU_LITERAL = "CPU";
  public static final String GPU_LITERAL = "GPU";

  private Map<String, Double> resources;

  public RayResources() {
    resources = new HashMap<>();
  }

  /**
   * Add a resource item.
   *
   * @param name The name of resource item.
   * @param value The value of resource item.
   */
  public void add(String name, Double value) {
    resources.put(name, value);
  }

  /**
   *
   * @param name The name of resource item.
   * @return The value of the resource item.
   */
  public Double get(String name) {
    if (!resources.containsKey(name)) {
      throw new IllegalArgumentException(String.format("Resource %s don't exist.", name));
    }

    return resources.get(name);
  }

  /**
   * Get the all of the resource items.
   *
   * @return the map which contains all of the resources items.
   */
  public Map<String, Double> getAll() {
    if (!resources.containsKey(CPU_LITERAL)) {
      resources.put(CPU_LITERAL, 0.0);
    }

    return resources;
  }

  @Override
  public String toString() {
    return String.format("Resource%s",resources.toString());
  }

}
