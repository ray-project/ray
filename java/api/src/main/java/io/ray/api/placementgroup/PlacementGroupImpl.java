package io.ray.api.placementgroup;

import io.ray.api.id.UniqueId;
import java.util.ArrayList;
import java.util.List;

/**
 * The default implementation of `PlacementGroup` interface.
 */
public class PlacementGroupImpl implements PlacementGroup {

  private List<Bundle> bundles = new ArrayList<>();

  public PlacementGroupImpl(UniqueId placementGroupId, int bundleCount) {
    for (int index = 0; index < bundleCount; ++index) {
      this.bundles.add(new Bundle(placementGroupId, index));
    }
  }

  @Override
  public Bundle getBundle(int index) {
    return bundles.get(index);
  }
}