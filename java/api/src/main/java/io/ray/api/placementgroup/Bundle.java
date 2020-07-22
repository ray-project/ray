package io.ray.api.placementgroup;

import io.ray.api.id.UniqueId;

public class Bundle {
  public UniqueId placementGroupId;
  public int bundleIndex;

  public Bundle(UniqueId placementGroupId, int bundleIndex) {
    this.placementGroupId = placementGroupId;
    this.bundleIndex = bundleIndex;
  }
}
