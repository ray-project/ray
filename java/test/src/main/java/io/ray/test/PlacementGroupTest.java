package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class PlacementGroupTest extends BaseTest {

  public static class Counter {

    private int value;

    public Counter(int initValue) {
      this.value = initValue;
    }

    public int getValue() {
      return value;
    }
  }

  // TODO(ffbin): Currently Java doesn't support multi-node tests.
  // This test just creates a placement group with one bundle.
  // It's not comprehensive to test all placement group test cases.
  public void testCreateAndCallActor() {
    List<Map<String, Double>> bundles = new ArrayList<>();
    Map<String, Double> bundle = new HashMap<>();
    bundle.put("CPU", 1.0);
    bundles.add(bundle);
    PlacementStrategy strategy = PlacementStrategy.PACK;
    PlacementGroup placementGroup = Ray.createPlacementGroup(bundles, strategy);

    // Test creating an actor from a constructor.
    ActorHandle<Counter> actor = Ray.actor(Counter::new, 1)
        .setPlacementGroup(placementGroup, 0).remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);

    // Test calling an actor.
    Assert.assertEquals(Integer.valueOf(1), actor.task(Counter::getValue).remote().get());
  }
}
