package org.ray.util;

import java.util.HashMap;
import java.util.Map;

/**
 * The utils of Resource.
 */
public class ResourceUtil {
  public static final String cpuLiteral = "CPU";
  public static final String gpuLiteral = "GPU";

  public static Map<String, Double> getResourcesMapFromArray(ResourceItem[] resourceArray) {
    Map<String, Double> resourceMap = new HashMap<>();
    if (resourceArray != null) {
      for (ResourceItem item : resourceArray) {
        if (!item.name().isEmpty()) {
          resourceMap.put(item.name(), item.value());
        }
      }
    }

    return resourceMap;
  }

  public static String getResourcesFromatStringFromMap(Map<String, Double> resources) {
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

  public static Map<String, Double> getResourcesMapFromString(String resources)
      throws IllegalArgumentException {
    Map<String, Double> ret = new HashMap<>();
    if (resources != null) {
      String[] items = resources.split(",");
      for (String item : items) {
        String trimItem = item.trim();
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
