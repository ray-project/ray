package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.PlacementGroups;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class PlacementGroupWithResourcesTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.head-args.0", "--num-cpus=1");
    // 1GB
    System.setProperty("ray.head-args.1", "--memory=1073741824");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.head-args.0");
    System.clearProperty("ray.head-args.1");
  }

  public static int simpleFunction() {
    return 1;
  }

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

  @Test
  public void testActorNoResources() {
    List<Map<String, Double>> bundles = new ArrayList<>();
    Map<String, Double> bundle = new HashMap<>();
    bundle.put("memory", 1073741824.0);
    bundles.add(bundle);
    PlacementGroupCreationOptions.Builder builder =
        new PlacementGroupCreationOptions.Builder()
            .setBundles(bundles)
            .setStrategy(PlacementStrategy.PACK);
    PlacementGroup placementGroup = PlacementGroups.createPlacementGroup(builder.build());
    Assert.assertTrue(placementGroup.wait(60));

    ActorHandle<Counter> actor =
        Ray.actor(Counter::new, 1).setPlacementGroup(placementGroup, 0).remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);
    Assert.assertEquals(actor.task(Counter::getValue).remote().get(3000), Integer.valueOf(1));

    Assert.assertEquals(
        Ray.task(PlacementGroupWithResourcesTest::simpleFunction)
            .setPlacementGroup(placementGroup, 0)
            .remote()
            .get(3000),
        Integer.valueOf(1));
  }
}
