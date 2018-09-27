package org.ray.runtime.util;

import java.util.HashMap;
import java.util.Map;
import org.ray.api.annotation.RayRemote;
import org.ray.api.annotation.ResourceItem;

public class ResourceUtil {
  public static final String CPU_LITERAL = "CPU";
  public static final String GPU_LITERAL = "GPU";

  /**
   * Convert the array that contains resource items to a map.
   *
   * @param remoteAnnotation The RayRemote annotation that contains the resource items.
   * @return The map whose key represents the resource name
   *     and the value represents the resource quantity.
   */
  public static Map<String, Double> getResourcesMapFromArray(RayRemote remoteAnnotation) {
    Map<String, Double> resourceMap = new HashMap<>();
    if (remoteAnnotation == null) {
      return resourceMap;
    }
    for (ResourceItem item : remoteAnnotation.resources()) {
      if (!item.name().isEmpty()) {
        resourceMap.put(item.name(), item.value());
      }
    }

    return resourceMap;
  }

  /**
   * Convert the resources map to a format string.
   *
   * @param resources The resource map to be Converted.
   * @return The format resources string, like "{CPU:4, GPU:0}".
   */
  public static String getResourcesFromatStringFromMap(Map<String, Double> resources) {
    if (resources == null) {
      return "{}";
    }
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    int count = 1;
    for (Map.Entry<String, Double> entry : resources.entrySet()) {
      builder.append(entry.getKey()).append(":").append(entry.getValue());
      count++;
      if (count != resources.size()) {
        builder.append(", ");
      }
    }
    builder.append("}");
    return builder.toString();
  }

  /**
   * Convert resources map to a string that is used
   * for the command line argument of starting raylet.
   *
   * @param resources The resources map to be converted.
   * @return The starting-raylet command line argument, like "CPU,4,GPU,0".
   */
  public static String getResourcesStringFromMap(Map<String, Double> resources) {
    StringBuilder builder = new StringBuilder();
    if (resources != null) {
      int count = 1;
      for (Map.Entry<String, Double> entry : resources.entrySet()) {
        builder.append(entry.getKey()).append(",").append(entry.getValue());
        if (count != resources.size()) {
          builder.append(",");
        }
        count++;
      }
    }
    return builder.toString();
  }

  /**
   * Parse the static resources configure field and convert to the resources map.
   *
   * @param resources The static resources string to be parsed.
   * @return The map whose key represents the resource name
   *     and the value represents the resource quantity.
   * @throws IllegalArgumentException If the resources string's format does match,
   *     it will throw an IllegalArgumentException.
   */
  public static Map<String, Double> getResourcesMapFromString(String resources)
      throws IllegalArgumentException {
    Map<String, Double> ret = new HashMap<>();
    if (resources != null) {
      String[] items = resources.split(",");
      for (String item : items) {
        String trimItem = item.trim();
        if (trimItem.isEmpty()) {
          continue;
        }
        String[] resourcePair = trimItem.split(":");

        if (resourcePair.length != 2) {
          throw new IllegalArgumentException("Format of static resurces configure is invalid.");
        }

        final String resourceName = resourcePair[0].trim();
        final Double resourceValue = Double.valueOf(resourcePair[1].trim());
        ret.put(resourceName, resourceValue);
      }
    }
    return ret;
  }
}
