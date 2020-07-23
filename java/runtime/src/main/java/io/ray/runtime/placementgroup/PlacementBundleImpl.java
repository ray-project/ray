package io.ray.runtime.placementgroup;

import io.ray.api.placementgroup.PlacementBundle;

/**
 * The default implementation of `PlacementBundle` interface.
 */
public class PlacementBundleImpl implements PlacementBundle {
  public final PlacementGroupId placementGroupId;
  public final int bundleIndex;

  public PlacementBundleImpl(PlacementGroupId placementGroupId, int bundleIndex) {
    this.placementGroupId = placementGroupId;
    this.bundleIndex = bundleIndex;
  }
}
