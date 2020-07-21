package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class PlacementGroupTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PlacementGroupTest.class);

  public static class Counter {

    private int value;

    public Counter(int initValue) {
      this.value = initValue;
    }

    public int getValue() {
      return value;
    }

    public void increase(int delta) {
      value += delta;
    }

    public int increaseAndGet(int delta) {
      value += delta;
      return value;
    }

    public int accessLargeObject(TestUtils.LargeObject largeObject) {
      value += largeObject.data.length;
      return value;
    }
  }

  public void testCreateAndCallActor() {
    List<Map<String, Double>> bundles = new ArrayList<>();
    Map<String, Double> bundle = new HashMap<>();
    bundle.put("R1", 4.0);
    bundles.add(bundle);
    PlacementStrategy strategy = PlacementStrategy.PACK;
    LOGGER.info("testCreateAndCallActor 11111111111111111");
    PlacementGroup placementGroup = Ray.createPlacementGroup(bundles, strategy);
    LOGGER.info("testCreateAndCallActor 22222222222222222");

    // Test creating an actor from a constructor
    ActorHandle<Counter> actor = Ray.actor(Counter::new, 1)
        .setBundle(placementGroup.getBundles().get(0)).remote();
    LOGGER.info("testCreateAndCallActor 3333333333333333");
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);
    LOGGER.info("testCreateAndCallActor 444444444444444");
    // A java actor is not a python actor
    Assert.assertFalse(actor instanceof PyActorHandle);
    LOGGER.info("testCreateAndCallActor 555555555555555");
    // Test calling an actor
    Assert.assertEquals(Integer.valueOf(1), actor.task(Counter::getValue).remote().get());
    LOGGER.info("testCreateAndCallActor 666666666666666");
    actor.task(Counter::increase, 1).remote();
    LOGGER.info("testCreateAndCallActor 777777777777777");
    Assert.assertEquals(Integer.valueOf(3),
        actor.task(Counter::increaseAndGet, 1).remote().get());
    LOGGER.info("testCreateAndCallActor 888888888888888");
  }
}
