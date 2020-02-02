package org.ray.api.options;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The options for creating a placement group.
 */
public class PlacementGroupOptions {

  public final List<SlotSetOptions> slotSets;

  public final PlacementStrategy strategy;

  private PlacementGroupOptions(List<SlotSetOptions> slotSets, PlacementStrategy strategy) {
    this.slotSets = slotSets;
    this.strategy = strategy;
  }

  /**
   * The inner class for building PlacementGroupOptions.
   */
  public static class Builder {

    private List<SlotSetOptions> slotSets = new ArrayList<>();
    private PlacementStrategy strategy;

    public Builder addSlotSet(int unitCount, Map<String, Double> unitResources) {
      this.slotSets.add(new SlotSetOptions.Builder().setUnitCount(unitCount)
          .setUnitResources(unitResources).createSlotSetOptions());
      return this;
    }

    public Builder setPlacementStrategy(PlacementStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public PlacementGroupOptions createPlacementGroupOptions() {
      return new PlacementGroupOptions(slotSets, strategy);
    }
  }
}
