package io.ray.api.placementgroup;

import io.ray.api.id.PlacementGroupId;

public class Bundle {
  public final PlacementGroupId placementGroupId;
  public final int bundleIndex;

  public Bundle(PlacementGroupId placementGroupId, int bundleIndex) {
    this.placementGroupId = placementGroupId;
    this.bundleIndex = bundleIndex;
  }
}
