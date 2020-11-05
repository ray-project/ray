package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import io.ray.runtime.placementgroup.PlacementGroupImpl;
import java.util.List;
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
    PlacementGroup placementGroup = PlacementGroupTestUtils.createSimpleGroup();
    Assert.assertEquals(((PlacementGroupImpl)placementGroup).getName(),"unnamed_group");

    // Test creating an actor from a constructor.
    ActorHandle<Counter> actor = Ray.actor(Counter::new, 1)
        .setPlacementGroup(placementGroup, 0).remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);

    // Test calling an actor.
    Assert.assertEquals(Integer.valueOf(1), actor.task(Counter::getValue).remote().get());
  }

  public void testGetPlacementGroup() {
    PlacementGroupImpl firstPlacementGroup = (PlacementGroupImpl)PlacementGroupTestUtils
        .createNameSpecifiedSimpleGroup("CPU", 1, PlacementStrategy.PACK,
        1.0, "first_placement_group");

    PlacementGroupImpl secondPlacementGroup = (PlacementGroupImpl)PlacementGroupTestUtils
        .createNameSpecifiedSimpleGroup("CPU", 1, PlacementStrategy.PACK,
        1.0, "second_placement_group");

    PlacementGroupImpl firstPlacementGroupRes =
        (PlacementGroupImpl)Ray.getPlacementGroup((firstPlacementGroup).getId());
    PlacementGroupImpl secondPlacementGroupRes =
        (PlacementGroupImpl)Ray.getPlacementGroup((secondPlacementGroup).getId());

    Assert.assertNotNull(firstPlacementGroupRes);
    Assert.assertNotNull(secondPlacementGroupRes);

    Assert.assertEquals(firstPlacementGroup.getId(), firstPlacementGroupRes.getId());
    Assert.assertEquals(firstPlacementGroup.getName(), firstPlacementGroupRes.getName());
    Assert.assertEquals(firstPlacementGroupRes.getBundles().size(), 1);
    Assert.assertEquals(firstPlacementGroupRes.getStrategy(), PlacementStrategy.PACK);

    List<PlacementGroup> allPlacementGroup = Ray.getAllPlacementGroup();
    Assert.assertEquals(allPlacementGroup.size(), 2);

    PlacementGroupImpl placementGroupRes = (PlacementGroupImpl)allPlacementGroup.get(0);
    Assert.assertNotNull(placementGroupRes.getId());
    PlacementGroupImpl expectPlacementGroup = placementGroupRes.getId()
        .equals(firstPlacementGroup.getId()) ? firstPlacementGroup : secondPlacementGroup;

    Assert.assertEquals(placementGroupRes.getName(), expectPlacementGroup.getName());
    Assert.assertEquals(placementGroupRes.getBundles().size(),
        expectPlacementGroup.getBundles().size());
    Assert.assertEquals(placementGroupRes.getStrategy(), expectPlacementGroup.getStrategy());
  }

  public void testCheckBundleIndex() {
    PlacementGroup placementGroup = PlacementGroupTestUtils.createSimpleGroup();

    int exceptionCount = 0;
    try {
      Ray.actor(Counter::new, 1).setPlacementGroup(placementGroup, 1).remote();
    } catch (IllegalArgumentException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(1, exceptionCount);

    try {
      Ray.actor(Counter::new, 1).setPlacementGroup(placementGroup, -1).remote();
    } catch (IllegalArgumentException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(2, exceptionCount);
  }

  @Test (expectedExceptions = { IllegalArgumentException.class })
  public void testBundleSizeValidCheckWhenCreate() {
    PlacementGroupTestUtils.createBundleSizeInvalidGroup();
  }

  @Test (expectedExceptions = { IllegalArgumentException.class })
  public void testBundleResourceValidCheckWhenCreate() {
    PlacementGroupTestUtils.createBundleResourceInvalidGroup();
  }
}
