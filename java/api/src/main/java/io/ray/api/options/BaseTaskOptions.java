package io.ray.api.options;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** The options class for RayCall or ActorCreation. */
public abstract class BaseTaskOptions implements Serializable {

  public final Map<String, Double> resources;

  public BaseTaskOptions() {
    resources = new HashMap<>();
  }

  public BaseTaskOptions(Map<String, Double> resources) {
    for (Map.Entry<String, Double> entry : resources.entrySet()) {
      if (entry.getValue() == null || entry.getValue().compareTo(0.0) <= 0) {
        throw new IllegalArgumentException(
            String.format(
                "Resource values should be " + "positive. Specified resource: %s = %s.",
                entry.getKey(), entry.getValue()));
      }
      // Note: A resource value should be an integer if it is greater than 1.0.
      // e.g. 3.0 is a valid resource value, but 3.5 is not.
      if (entry.getValue().compareTo(1.0) >= 0
          && entry.getValue().compareTo(Math.floor(entry.getValue())) != 0) {
        throw new IllegalArgumentException(
            String.format(
                "A resource value should be "
                    + "an integer if it is greater than 1.0. Specified resource: %s = %s.",
                entry.getKey(), entry.getValue()));
      }
    }
    this.resources = resources;
  }
}
