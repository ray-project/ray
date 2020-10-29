package io.ray.runtime.placementgroup;

import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.List;
import java.util.Map;

/**
 * The default implementation of `PlacementGroup` interface.
 */
public class PlacementGroupImpl implements PlacementGroup {

  private final PlacementGroupId id;
  private final String name;
  private final List<Map<String, Double>> bundles;
  private final PlacementStrategy strategy;

  public PlacementGroupImpl(PlacementGroupId id, String name,
                            List<Map<String, Double>> bundles,
                            PlacementStrategy strategy) {
    this.id = id;
    this.name = name;
    this.bundles = bundles;
    this.strategy = strategy;
  }

  public PlacementGroupId getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public List<Map<String, Double>> getBundles() {
    return bundles;
  }

  public PlacementStrategy getStrategy() {
    return strategy;
  }

  /**
   * A help class for create the Placement Group.
   */
  public static class Builder {
    private PlacementGroupId id;
    private String name;
    private List<Map<String, Double>> bundles;
    private PlacementStrategy strategy;

    /**
     * Set the Id of the Placement Group.
     * @param id Id of the Placement Group.
     * @return self.
     */
    public Builder setId(PlacementGroupId id) {
      this.id = id;
      return this;
    }

    /**
     * Set the name of the Placement Group.
     * @param name Name of the Placement Group.
     * @return self.
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the bundles of the Placement Group.
     * @param bundles the bundles of the Placement Group.
     * @return self.
     */
    public Builder setBundles(List<Map<String, Double>> bundles) {
      this.bundles = bundles;
      return this;
    }

    /**
     * Set the placement strategy of the Placement Group.
     * @param strategy the placement strategy of the Placement Group.
     * @return self.
     */
    public Builder setStrategy(PlacementStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public PlacementGroupImpl build() {
      return new PlacementGroupImpl(id, name, bundles, strategy);
    }
  }

}
