package io.ray.api.placementgroup;

import java.util.List;
import java.util.Map;

/**
 * A handle to a placement group.
 */
public class PlacementGroupImpl implements PlacementGroup {
  private List<Map<String, Double>> bundles;

  public PlacementGroupImpl(List<Map<String, Double>> bundles) {
    this.bundles = bundles;
  }

  /**
   * @return All bundles in this group.
   */
  @Override
  public List<Map<String, Double>> getBundles() {
    return bundles;
  }
}