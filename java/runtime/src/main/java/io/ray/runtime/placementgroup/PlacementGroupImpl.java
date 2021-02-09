package io.ray.runtime.placementgroup;

import io.ray.api.Ray;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementGroupState;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.List;
import java.util.Map;

/** The default implementation of `PlacementGroup` interface. */
public class PlacementGroupImpl implements PlacementGroup {

  private final PlacementGroupId id;
  private final String name;
  private final List<Map<String, Double>> bundles;
  private final PlacementStrategy strategy;
  private final PlacementGroupState state;

  private PlacementGroupImpl(
      PlacementGroupId id,
      String name,
      List<Map<String, Double>> bundles,
      PlacementStrategy strategy,
      PlacementGroupState state) {
    this.id = id;
    this.name = name;
    this.bundles = bundles;
    this.strategy = strategy;
    this.state = state;
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

  public PlacementGroupState getState() {
    return state;
  }

  /**
   * Wait for the placement group to be ready within the specified time.
   *
   * @param timeoutSeconds Timeout in seconds. Returns True if the placement group is created. False
   *     otherwise.
   */
  public boolean wait(int timeoutSeconds) {
    return Ray.internal().waitPlacementGroupReady(id, timeoutSeconds);
  }

  /** A help class for create the placement group. */
  public static class Builder {
    private PlacementGroupId id;
    private String name;
    private List<Map<String, Double>> bundles;
    private PlacementStrategy strategy;
    private PlacementGroupState state;

    /**
     * Set the Id of the placement group.
     *
     * @param id Id of the placement group. Returns self.
     */
    public Builder setId(PlacementGroupId id) {
      this.id = id;
      return this;
    }

    /**
     * Set the name of the placement group.
     *
     * @param name Name of the placement group. Returns self.
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the bundles of the placement group.
     *
     * @param bundles the bundles of the placement group. Returns self.
     */
    public Builder setBundles(List<Map<String, Double>> bundles) {
      this.bundles = bundles;
      return this;
    }

    /**
     * Set the placement strategy of the placement group.
     *
     * @param strategy the placement strategy of the placement group. Returns self.
     */
    public Builder setStrategy(PlacementStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    /**
     * Set the placement state of the placement group.
     *
     * @param state the state of the placement group. Returns self.
     */
    public Builder setState(PlacementGroupState state) {
      this.state = state;
      return this;
    }

    public PlacementGroupImpl build() {
      return new PlacementGroupImpl(id, name, bundles, strategy, state);
    }
  }
}
