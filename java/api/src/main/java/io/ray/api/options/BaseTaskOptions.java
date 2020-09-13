package io.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options class for RayCall or ActorCreation.
 */
public abstract class BaseTaskOptions {

  public final Map<String, Double> resources;

  public BaseTaskOptions() {
    resources = new HashMap<>();
  }

  public BaseTaskOptions(Map<String, Double> resources) {
    for (Map.Entry<String, Double> entry : resources.entrySet()) {
      if (entry.getValue().compareTo(0.0) <= 0) {
        throw new IllegalArgumentException(String.format("Resource capacity should be " +
            "positive, but got resource %s = %f.", entry.getKey(), entry.getValue()));
      }
    }
    this.resources = resources;
  }

}
