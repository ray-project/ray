package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options class for RayCall or ActorCreation.
 */
public abstract class BaseTaskOptions {
  // DO NOT set this environment variable. It's only used for test purposes.
  // Please use `setUseDirectCall` instead.
  public static final boolean DEFAULT_USE_DIRECT_CALL = "1"
      .equals(System.getenv("DEFAULT_USE_DIRECT_CALL"));

  public final Map<String, Double> resources;

  public final boolean useDirectCall;

  public BaseTaskOptions() {
    resources = new HashMap<>();
    useDirectCall = DEFAULT_USE_DIRECT_CALL;
  }

  public BaseTaskOptions(Map<String, Double> resources, boolean useDirectCall) {
    for (Map.Entry<String, Double> entry : resources.entrySet()) {
      if (entry.getValue().compareTo(0.0) <= 0) {
        throw new IllegalArgumentException(String.format("Resource capacity should be " +
            "positive, but got resource %s = %f.", entry.getKey(), entry.getValue()));
      }
    }
    this.resources = resources;
    this.useDirectCall = useDirectCall;
  }

}
