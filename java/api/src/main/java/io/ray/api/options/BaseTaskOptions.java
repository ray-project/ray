package io.ray.api.options;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** The options class for RayCall or ActorCreation. */
public abstract class BaseTaskOptions implements Serializable {

  private final Map<String, Double> resources;

  public BaseTaskOptions(Map<String, Double> resources) {
    if (resources == null) {
      throw new IllegalArgumentException("Resources map should not be null!");
    }

    Map<String, Double> filteredResources = validateAndFilterResources(resources);
    this.resources = Collections.unmodifiableMap(filteredResources);
  }

  private Map<String, Double> validateAndFilterResources(Map<String, Double> resources) {
    Map<String, Double> filtered = new HashMap<>();
    for (Map.Entry<String, Double> entry : resources.entrySet()) {
      String name = entry.getKey();
      Double value = entry.getValue();

      validateResourceValue(name, value);
      validateIntegerConstraint(name, value);

      if (value != 0.0) {
        filtered.put(name, value);
      }
    }
    return filtered;
  }

  private void validateResourceValue(String name, Double value) {
    if (name == null || value == null) {
      throw new IllegalArgumentException(
          String.format(
              "Resource name and value should not be null. Specified resource: %s = %s.",
              name, value));
    } else if (value < 0.0) {
      throw new IllegalArgumentException(
          String.format(
              "Resource values should be non negative. Specified resource: %s = %s.", name, value));
    }
  }

  /**
   * Validates that resource values greater than or equal to 1.0 are integers.
   *
   * @param name the name of the resource being validated
   * @param value the value of the resource to validate
   * @throws IllegalArgumentException if the value is >= 1.0 and not an integer
   */
  private void validateIntegerConstraint(String name, Double value) {
    if (value >= 1.0 && value % 1.0 != 0.0) {
      throw new IllegalArgumentException(
          String.format(
              "A resource value should be an integer if it is greater than 1.0. Specified resource: %s = %s",
              name, value));
    }
  }

  public Map<String, Double> getResources() {
    return resources;
  }
}
