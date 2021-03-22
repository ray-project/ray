package io.ray.docdemo;

import io.ray.api.Ray;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlacementGroupDemo {

  public static void createPlacementGroup() {
    // Initialize Ray.
    Ray.init();

    // Construct a list of bundles.
    List<Map<String, Double>> bundles = new ArrayList<>();
    Map<String, Double> bundle = new HashMap<>();
    bundle.put("CPU", 1.0);

    bundles.add(bundle);

    // Make a creation option with bundles and strategy.
    PlacementGroupCreationOptions options =
      new PlacementGroupCreationOptions.Builder()
        .setBundles(bundles)
        .setStrategy(PlacementStrategy.STRICT_SPREAD)
        .build();

    PlacementGroup pg = Ray.createPlacementGroup(options);

    // Wait for the placement group to be ready within the specified time(unit is seconds).
    boolean ready = pg.wait(60);

    // You can look at placement group states using this API.
    List<PlacementGroup> allPlacementGroup = Ray.getAllPlacementGroups();
    for (PlacementGroup group: allPlacementGroup) {
      System.out.println(group);
    }
  }

  public static void main(String[] args) {
    // Start Ray runtime. If you're connecting to an existing cluster, you can set
    // the `-Dray.address=<cluster-address>` java system property.
    Ray.init();

    createPlacementGroup();
  }
}
