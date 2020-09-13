package io.ray.runtime.placementgroup;

import io.ray.api.placementgroup.PlacementGroup;

/**
 * The default implementation of `PlacementGroup` interface.
 */
public class PlacementGroupImpl implements PlacementGroup {

  private PlacementGroupId id;
  private int bundleCount = 0;

  public PlacementGroupImpl() {
  }

  public PlacementGroupImpl(PlacementGroupId id, int bundleCount) {
    this.id = id;
    this.bundleCount = bundleCount;
  }

  public PlacementGroupId getId() {
    return id;
  }

  public int getBundleCount() {
    return bundleCount;
  }
}
