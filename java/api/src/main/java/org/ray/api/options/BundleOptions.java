package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for creating a bundle. ONLY for internal use.
 */
public class BundleOptions extends BaseTaskOptions {

  public final int unitCount;

  private BundleOptions(Map<String, Double> unitResources, int unitCount) {
    super(unitResources);
    this.unitCount = unitCount;
  }

  /**
   * The inner class for building BundleOptions.
   */
  static class Builder {

    private Map<String, Double> unitResources = new HashMap<>();
    private int unitCount = 1;

    public Builder setUnitCount(int unitCount) {
      this.unitCount = unitCount;
      return this;
    }

    public Builder setUnitResources(Map<String, Double> unitResources) {
      this.unitResources = unitResources;
      return this;
    }

    public BundleOptions createBundleOptions() {
      return new BundleOptions(unitResources, unitCount);
    }
  }
}
