package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementGroupState;
import io.ray.api.placementgroup.PlacementStrategy;
import io.ray.runtime.exception.RayException;
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
    Assert.assertTrue(placementGroup.wait(10));
    Assert.assertEquals(placementGroup.getName(), "unnamed_group");

    // Test creating an actor from a constructor.
    ActorHandle<Counter> actor =
        Ray.actor(Counter::new, 1).setPlacementGroup(placementGroup, 0).remote();
    Assert.assertNotEquals(actor.getId(), ActorId.NIL);

    // Test calling an actor.
    Assert.assertEquals(actor.task(Counter::getValue).remote().get(), Integer.valueOf(1));
  }

  @Test(groups = {"cluster"})
  public void testGetPlacementGroup() {
    PlacementGroup firstPlacementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, "first_placement_group");

    PlacementGroup secondPlacementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, "second_placement_group");
    Assert.assertTrue(firstPlacementGroup.wait(10));
    Assert.assertTrue(secondPlacementGroup.wait(10));

    PlacementGroup firstPlacementGroupRes = Ray.getPlacementGroup((firstPlacementGroup).getId());
    PlacementGroup secondPlacementGroupRes = Ray.getPlacementGroup((secondPlacementGroup).getId());

    Assert.assertNotNull(firstPlacementGroupRes);
    Assert.assertNotNull(secondPlacementGroupRes);

    Assert.assertEquals(firstPlacementGroup.getId(), firstPlacementGroupRes.getId());
    Assert.assertEquals(firstPlacementGroup.getName(), firstPlacementGroupRes.getName());
    Assert.assertEquals(firstPlacementGroupRes.getBundles().size(), 1);
    Assert.assertEquals(firstPlacementGroupRes.getStrategy(), PlacementStrategy.PACK);

    List<PlacementGroup> allPlacementGroup = Ray.getAllPlacementGroups();
    Assert.assertEquals(allPlacementGroup.size(), 2);

    PlacementGroup placementGroupRes = allPlacementGroup.get(0);
    Assert.assertNotNull(placementGroupRes.getId());
    PlacementGroup expectPlacementGroup =
        placementGroupRes.getId().equals(firstPlacementGroup.getId())
            ? firstPlacementGroup
            : secondPlacementGroup;

    Assert.assertEquals(placementGroupRes.getName(), expectPlacementGroup.getName());
    Assert.assertEquals(
        placementGroupRes.getBundles().size(), expectPlacementGroup.getBundles().size());
    Assert.assertEquals(placementGroupRes.getStrategy(), expectPlacementGroup.getStrategy());
  }

  @Test(
      groups = {"cluster"},
      enabled = false)
  public void testRemovePlacementGroup() {
    PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
        "CPU", 1, PlacementStrategy.PACK, 1.0, "first_placement_group");

    PlacementGroup secondPlacementGroup =
        PlacementGroupTestUtils.createNameSpecifiedSimpleGroup(
            "CPU", 1, PlacementStrategy.PACK, 1.0, "second_placement_group");

    List<PlacementGroup> allPlacementGroup = Ray.getAllPlacementGroups();
    Assert.assertEquals(allPlacementGroup.size(), 2);

    Ray.removePlacementGroup(secondPlacementGroup.getId());

    PlacementGroup removedPlacementGroup = Ray.getPlacementGroup((secondPlacementGroup).getId());
    Assert.assertEquals(removedPlacementGroup.getState(), PlacementGroupState.REMOVED);

    // Wait for placement group after it is removed.
    int exceptionCount = 0;
    try {
      removedPlacementGroup.wait(10);
    } catch (RayException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(exceptionCount, 1);
  }

  public void testCheckBundleIndex() {
    PlacementGroup placementGroup = PlacementGroupTestUtils.createSimpleGroup();

    int exceptionCount = 0;
    try {
      Ray.actor(Counter::new, 1).setPlacementGroup(placementGroup, 1).remote();
    } catch (IllegalArgumentException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(exceptionCount, 1);

    try {
      Ray.actor(Counter::new, 1).setPlacementGroup(placementGroup, -1).remote();
    } catch (IllegalArgumentException e) {
      ++exceptionCount;
    }
    Assert.assertEquals(exceptionCount, 2);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testBundleSizeValidCheckWhenCreate() {
    PlacementGroupTestUtils.createBundleSizeInvalidGroup();
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testBundleResourceValidCheckWhenCreate() {
    PlacementGroupTestUtils.createBundleResourceInvalidGroup();
  }
}
