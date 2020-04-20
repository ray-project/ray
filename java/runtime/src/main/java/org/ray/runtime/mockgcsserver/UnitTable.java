package org.ray.runtime.mockgcsserver;

import java.util.Map;

public class UnitTable {

  public final Map<String, Double> availableResources;

  public final String label;

  public UnitTable(Map<String, Double> availableResources, String label) {
    this.availableResources = availableResources;
    this.label = label;
  }
}
