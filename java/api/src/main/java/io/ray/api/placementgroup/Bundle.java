package io.ray.api.placementgroup;

import io.ray.api.id.PlacementGroupId;

public class Bundle {
  public PlacementGroupId placementGroupId;
  public int bundleIndex;

  public Bundle(PlacementGroupId placementGroupId, int bundleIndex) {
    this.placementGroupId = placementGroupId;
    this.bundleIndex = bundleIndex;
  }

  public PlacementGroupId getPlacementGroupId() {
    return placementGroupId;
  }

  public void setPlacementGroupId(PlacementGroupId placementGroupId) {
    this.placementGroupId = placementGroupId;
  }

  public int getBundleIndex() {
    return bundleIndex;
  }

  public void setBundleIndex(int bundleIndex) {
    this.bundleIndex = bundleIndex;
  }
}
