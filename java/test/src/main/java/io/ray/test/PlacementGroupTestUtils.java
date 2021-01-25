package io.ray.test;

import io.ray.api.Ray;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A utils class for placement group test. */
public class PlacementGroupTestUtils {

  public static PlacementGroup createNameSpecifiedSimpleGroup(
      String resourceName,
      int bundleSize,
      PlacementStrategy strategy,
      Double resourceSize,
      String groupName) {
    List<Map<String, Double>> bundles = new ArrayList<>();

    for (int i = 0; i < bundleSize; i++) {
      Map<String, Double> bundle = new HashMap<>();
      bundle.put(resourceName, resourceSize);
      bundles.add(bundle);
    }

    return Ray.createPlacementGroup(groupName, bundles, strategy);
  }

  public static PlacementGroup createSpecifiedSimpleGroup(
      String resourceName, int bundleSize, PlacementStrategy strategy, Double resourceSize) {
    return createNameSpecifiedSimpleGroup(
        resourceName, bundleSize, strategy, resourceSize, "unnamed_group");
  }

  public static PlacementGroup createSimpleGroup() {
    return createSpecifiedSimpleGroup("CPU", 1, PlacementStrategy.PACK, 1.0);
  }

  public static void createBundleSizeInvalidGroup() {
    createSpecifiedSimpleGroup("CPU", 0, PlacementStrategy.PACK, 1.0);
  }

  public static void createBundleResourceInvalidGroup() {
    createSpecifiedSimpleGroup("CPU", 1, PlacementStrategy.PACK, 0.0);
  }
}
