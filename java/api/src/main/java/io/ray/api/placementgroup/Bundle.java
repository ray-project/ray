package io.ray.api.placementgroup;

import io.ray.api.id.PlacementGroupId;

public class Bundle {
  public PlacementGroupId placementGroupId;
  public int bundleIndex;

  public Bundle(PlacementGroupId placementGroupId, int bundleIndex) {
    this.placementGroupId = placementGroupId;
    this.bundleIndex = bundleIndex;
  }
}
