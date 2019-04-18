package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options class for RayCall or ActorCreation.
 */
public abstract class BaseTaskOptions {
  public Map<String, Double> resources;

  public BaseTaskOptions() {
    resources = new HashMap<>();
  }

  public BaseTaskOptions(Map<String, Double> resources) {
    this.resources = resources;
  }

}
