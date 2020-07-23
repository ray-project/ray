package io.ray.runtime.placementgroup;

import io.ray.api.placementgroup.PlacementBundle;
import io.ray.api.placementgroup.PlacementGroup;
import java.util.ArrayList;
import java.util.List;

/**
 * The default implementation of `PlacementGroup` interface.
 */
public class PlacementGroupImpl implements PlacementGroup {

  private List<PlacementBundle> placementBundles = new ArrayList<>();

  public PlacementGroupImpl() {
  }

  public PlacementGroupImpl(PlacementGroupId placementGroupId, int bundleCount) {
    for (int index = 0; index < bundleCount; ++index) {
      this.placementBundles.add(new PlacementBundleImpl(placementGroupId, index));
    }
  }

  @Override
  public PlacementBundle getBundle(int index) {
    return placementBundles.get(index);
  }
}
