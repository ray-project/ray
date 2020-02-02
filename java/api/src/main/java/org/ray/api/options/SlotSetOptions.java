package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for creating a slot set. ONLY for internal use.
 */
public class SlotSetOptions extends BaseTaskOptions {

  public final int unitCount;

  private SlotSetOptions(int unitCount, Map<String, Double> unitResources) {
    super(unitResources);
    this.unitCount = unitCount;
  }

  /**
   * The inner class for building SlotSetOptions.
   */
  static class Builder {

    private int unitCount = 1;
    private Map<String, Double> unitResources = new HashMap<>();

    public Builder setUnitCount(int unitCount) {
      this.unitCount = unitCount;
      return this;
    }

    public Builder setUnitResources(Map<String, Double> unitResources) {
      this.unitResources = unitResources;
      return this;
    }

    public SlotSetOptions createSlotSetOptions() {
      return new SlotSetOptions(unitCount, unitResources);
    }
  }
}
