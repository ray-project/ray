package io.ray.api.placementgroup;

import java.util.List;
import java.util.Map;

/**
 * A handle to a placement group.
 */
public interface PlacementGroup {


  /**
   * @return All bundles in this group.
   */
  List<Map<String, Double>> getBundles();
}