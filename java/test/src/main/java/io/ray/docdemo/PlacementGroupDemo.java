package io.ray.docdemo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.ray.api.ObjectRef;
import io.ray.api.PlacementGroups;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementGroupState;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.List;
import java.util.Map;
import org.testng.Assert;

public class PlacementGroupDemo {

  public static class Counter {
    private int value;

    public Counter(int initValue) {
      this.value = initValue;
    }

    public int getValue() {
      return value;
    }

    public static String ping() {
      return "pong";
    }
  }

  public static void createAndRemovePlacementGroup() {
    // Construct a list of bundles.
    Map<String, Double> bundle = ImmutableMap.of("CPU", 1.0);
    List<Map<String, Double>> bundles = ImmutableList.of(bundle);

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
    Map<String, Double> bundle = ImmutableMap.of("CPU", 2.0);
    List<Map<String, Double>> bundles = ImmutableList.of(bundle);

    // Create a placement group and make sure its creation is successful.
    PlacementGroupCreationOptions options =
        new PlacementGroupCreationOptions.Builder()
            .setBundles(bundles)
            .setStrategy(PlacementStrategy.STRICT_SPREAD)
            .build();

    PlacementGroup pg = PlacementGroups.createPlacementGroup(options);
    boolean isCreated = pg.wait(60);
    Assert.assertTrue(isCreated);

    // Won't be scheduled because there are no 2 cpus now.
    ObjectRef<String> obj = Ray.task(Counter::ping).setResource("CPU", 2.0).remote();

    List<ObjectRef<String>> waitList = ImmutableList.of(obj);
    WaitResult<String> waitResult = Ray.wait(waitList, 1, 5 * 1000);
    Assert.assertEquals(1, waitResult.getUnready().size());

    // Will be scheduled because 2 cpus are reserved by the placement group.
    obj = Ray.task(Counter::ping).setPlacementGroup(pg, 0).setResource("CPU", 2.0).remote();
    Assert.assertEquals(obj.get(), "pong");

    PlacementGroups.removePlacementGroup(pg.getId());

    PlacementGroup removedPlacementGroup = PlacementGroups.getPlacementGroup(pg.getId());
    Assert.assertEquals(removedPlacementGroup.getState(), PlacementGroupState.REMOVED);
  }

  public static void createNonGlobalNamedPlacementGroup() {
    // Create a placement group with a job-scope-unique name.
    Map<String, Double> bundle = ImmutableMap.of("CPU", 1.0);
    List<Map<String, Double>> bundles = ImmutableList.of(bundle);

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

  public static void strictPackExample() {
    Map<String, Double> bundle1 = ImmutableMap.of("GPU", 2.0);
    Map<String, Double> bundle2 = ImmutableMap.of("extra_resource", 2.0);
    List<Map<String, Double>> bundles = ImmutableList.of(bundle1, bundle2);

    PlacementGroupCreationOptions options =
        new PlacementGroupCreationOptions.Builder()
            .setBundles(bundles)
            .setStrategy(PlacementStrategy.STRICT_PACK)
            .build();

    PlacementGroup pg = PlacementGroups.createPlacementGroup(options);
    boolean isCreated = pg.wait(60);
    Assert.assertTrue(isCreated);

    // Create GPU actors on a gpu bundle.
    for (int index = 0; index < 2; index++) {
      Ray.actor(Counter::new, 1).setResource("GPU", 1.0).setPlacementGroup(pg, 0).remote();
    }

    // Create extra_resource actors on a extra_resource bundle.
    for (int index = 0; index < 2; index++) {
      Ray.task(Counter::ping)
          .setPlacementGroup(pg, 1)
          .setResource("extra_resource", 1.0)
          .remote()
          .get();
    }
  }

  public static void main(String[] args) {
    // Start Ray runtime. If you're connecting to an existing cluster, you can set
    // the `-Dray.address=<cluster-address>` java system property.
    System.setProperty("ray.head-args.0", "--resources={\"extra_resource\":2.0}");
    System.setProperty("ray.head-args.1", "--num-cpus=2");
    System.setProperty("ray.head-args.2", "--num-gpus=2");
    Ray.init();

    createAndRemovePlacementGroup();

    runNormalTaskWithPlacementGroup();

    createNonGlobalNamedPlacementGroup();

    strictPackExample();
  }
}
