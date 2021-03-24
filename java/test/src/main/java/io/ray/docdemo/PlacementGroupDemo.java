package io.ray.docdemo;

import io.ray.api.PlacementGroups;
import io.ray.api.Ray;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementGroupState;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;

public class PlacementGroupDemo {

  public static class Counter {
    public static String ping() {
      return "pong";
    }
  }

  public static void createAndRemovePlacementGroup() {
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

    PlacementGroup pg = PlacementGroups.createPlacementGroup(options);

    // Wait for the placement group to be ready within the specified time(unit is seconds).
    boolean ready = pg.wait(60);
    Assert.assertTrue(ready);

    // You can look at placement group states using this API.
    List<PlacementGroup> allPlacementGroup = PlacementGroups.getAllPlacementGroups();
    for (PlacementGroup group : allPlacementGroup) {
      System.out.println(group);
    }

    PlacementGroups.removePlacementGroup(pg.getId());

    PlacementGroup removedPlacementGroup = PlacementGroups.getPlacementGroup(pg.getId());
    Assert.assertEquals(removedPlacementGroup.getState(), PlacementGroupState.REMOVED);
  }

  public static void runNormalTaskWithPlacementGroup() {
    // Construct a list of bundles.
    List<Map<String, Double>> bundles = new ArrayList<>();
    Map<String, Double> bundle = new HashMap<>();
    bundle.put("CPU", 1.0);
    bundles.add(bundle);

    // Create a placement group and make sure its creation is successful.
    PlacementGroupCreationOptions options =
        new PlacementGroupCreationOptions.Builder()
            .setBundles(bundles)
            .setStrategy(PlacementStrategy.STRICT_SPREAD)
            .build();

    PlacementGroup pg = PlacementGroups.createPlacementGroup(options);
    boolean isCreated = pg.wait(60);
    Assert.assertTrue(isCreated);

    // Will be scheduled because 2 cpus are reserved by the placement group.
    Ray.task(Counter::ping).setPlacementGroup(pg, 0).setResource("CPU", 1.0).remote();

    PlacementGroups.removePlacementGroup(pg.getId());

    PlacementGroup removedPlacementGroup = PlacementGroups.getPlacementGroup(pg.getId());
    Assert.assertEquals(removedPlacementGroup.getState(), PlacementGroupState.REMOVED);
  }

  public static void createGlobalNamedPlacementGroup() {
    // Create a placement group with a globally unique name.
    List<Map<String, Double>> bundles = new ArrayList<>();
    Map<String, Double> bundle1 = new HashMap<>();
    bundle1.put("CPU", 1.0);
    bundles.add(bundle1);

    PlacementGroupCreationOptions options =
        new PlacementGroupCreationOptions.Builder()
            .setBundles(bundles)
            .setStrategy(PlacementStrategy.STRICT_SPREAD)
            .setGlobalName("global_name")
            .build();

    PlacementGroup pg = PlacementGroups.createPlacementGroup(options);
    pg.wait(60);

    // Retrieve the placement group later somewhere.
    PlacementGroup group = PlacementGroups.getGlobalPlacementGroup("global_name");
    Assert.assertNotNull(group);

    PlacementGroups.removePlacementGroup(pg.getId());

    PlacementGroup removedPlacementGroup = PlacementGroups.getPlacementGroup(pg.getId());
    Assert.assertEquals(removedPlacementGroup.getState(), PlacementGroupState.REMOVED);
  }

  public static void createNonGlobalNamedPlacementGroup() {
    // Create a placement group with a job-scope-unique name.
    List<Map<String, Double>> bundles = new ArrayList<>();
    Map<String, Double> bundle1 = new HashMap<>();
    bundle1.put("CPU", 1.0);
    bundles.add(bundle1);

    PlacementGroupCreationOptions options =
        new PlacementGroupCreationOptions.Builder()
            .setBundles(bundles)
            .setStrategy(PlacementStrategy.STRICT_SPREAD)
            .setName("non_global_name")
            .build();

    PlacementGroup pg = PlacementGroups.createPlacementGroup(options);
    pg.wait(60);

    // Retrieve the placement group later somewhere in the same job.
    PlacementGroup group = PlacementGroups.getPlacementGroup("non_global_name");
    Assert.assertNotNull(group);

    PlacementGroups.removePlacementGroup(pg.getId());

    PlacementGroup removedPlacementGroup = PlacementGroups.getPlacementGroup(pg.getId());
    Assert.assertEquals(removedPlacementGroup.getState(), PlacementGroupState.REMOVED);
  }

  public static void main(String[] args) {
    // Start Ray runtime. If you're connecting to an existing cluster, you can set
    // the `-Dray.address=<cluster-address>` java system property.
    Ray.init();

    createAndRemovePlacementGroup();

    runNormalTaskWithPlacementGroup();

    createGlobalNamedPlacementGroup();

    createNonGlobalNamedPlacementGroup();
  }
}
