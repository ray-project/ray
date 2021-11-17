package io.ray.api.options;

import io.ray.api.Ray;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.List;
import java.util.Map;

/** The options for creating placement group. */
public class PlacementGroupCreationOptions {
  public final String name;
  public final List<Map<String, Double>> bundles;
  public final PlacementStrategy strategy;

  public PlacementGroupCreationOptions(
      String name, List<Map<String, Double>> bundles, PlacementStrategy strategy) {
    if (bundles == null || bundles.isEmpty()) {
      throw new IllegalArgumentException(
          "`Bundles` must be specified when creating a new placement group.");
    }
    boolean bundleResourceValid =
        bundles.stream()
            .allMatch(bundle -> bundle.values().stream().allMatch(resource -> resource > 0));

    if (!bundleResourceValid) {
      throw new IllegalArgumentException(
          "Bundles cannot be empty or bundle's resource must be positive.");
    }
    if (strategy == null) {
      throw new IllegalArgumentException(
          "`PlacementStrategy` must be specified when creating a new placement group.");
    }
    this.name = name;
    this.bundles = bundles;
    this.strategy = strategy;
  }

  /** The inner class for building PlacementGroupCreationOptions. */
  public static class Builder {
    private String name;
    private List<Map<String, Double>> bundles;
    private PlacementStrategy strategy;

    /**
     * Set the name of a named placement group. This named placement group is accessible in this
     * namespace by this name via {@link Ray#getPlacementGroup(java.lang.String)} or in other
     * namespaces via {@link PlacementGroups#getPlacementGroup(java.lang.String, java.lang.String)}.
     *
     * @param name The name of the named placement group.
     * @return self
     */
    public Builder setName(String name) {
      if (this.name != null) {
        throw new IllegalArgumentException("Repeated assignment of the name is not allowed!");
      }
      this.name = name;
      return this;
    }

    /**
     * Set the Pre-allocated resource list. Bundle is a collection of resources used to reserve
     * resources on the raylet side.
     *
     * @param bundles The Pre-allocated resource list.
     * @return self
     */
    public Builder setBundles(List<Map<String, Double>> bundles) {
      this.bundles = bundles;
      return this;
    }

    /**
     * Set the placement strategy used to control the placement relationship between bundles. More
     * details refer to {@link PlacementStrategy}
     *
     * @param strategy The placement strategy between bundles.
     * @return self
     */
    public Builder setStrategy(PlacementStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public PlacementGroupCreationOptions build() {
      return new PlacementGroupCreationOptions(name, bundles, strategy);
    }
  }
}
