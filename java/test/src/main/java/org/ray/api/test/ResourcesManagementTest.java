package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.api.WaitResult;
import org.ray.core.RayRuntime;
import org.ray.util.ResourceItem;

/**
 * Resources Management Test.
 */
@RunWith(MyRunner.class)
public class ResourcesManagementTest {

  @RayRemote(resources = {@ResourceItem(name = "CPU", value = 4),
      @ResourceItem(name = "GPU", value = 0)})
  public static Integer echo1(Integer number) {
    return number;
  }

  @RayRemote(resources = {@ResourceItem(name = "CPU", value = 4),
      @ResourceItem(name = "GPU", value = 2)})
  public static Integer echo2(Integer number) {
    return number;
  }

  @RayRemote(resources = {@ResourceItem(name = "CPU", value = 2),
      @ResourceItem(name = "GPU", value = 0)})
  public static class Echo1 {
    public Integer echo(Integer number) {
      return number;
    }
  }

  @RayRemote(resources = {@ResourceItem(name = "CPU", value = 8),
      @ResourceItem(name = "GPU", value = 0)})
  public static class Echo2 {
    public Integer echo(Integer number) {
      return number;
    }
  }

  @Test
  public void testMethods() {
    Assume.assumeTrue(RayRuntime.getParams().use_raylet);
    // This is a case that can satisfy required resources.
    RayObject<Integer> result1 = Ray.call(ResourcesManagementTest::echo1, 100);
    Assert.assertEquals(100, (int) result1.get());

    // This is a case that can't satisfy required resources.
    final RayObject<Integer> result2 = Ray.call(ResourcesManagementTest::echo2, 200);
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result2), 1, 1000);

    Assert.assertEquals(0, waitResult.getReadyOnes().size());
    Assert.assertEquals(1, waitResult.getRemainOnes().size());
  }

  @Test
  public void testActors() {
    Assume.assumeTrue(RayRuntime.getParams().use_raylet);
    // This is a case that can satisfy required resources.
    RayActor<ResourcesManagementTest.Echo1> echo1 = Ray.create(Echo1.class);
    final RayObject<Integer> result1 = Ray.call(Echo1::echo, echo1, 100);
    Assert.assertEquals(100, (int) result1.get());

    // This is a case that can't satisfy required resources.
    RayActor<ResourcesManagementTest.Echo2> echo2 = Ray.create(Echo2.class);
    final RayObject<Integer> result2 = Ray.call(Echo2::echo, echo2, 100);
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result2), 1, 1000);

    Assert.assertEquals(0, waitResult.getReadyOnes().size());
    Assert.assertEquals(1, waitResult.getRemainOnes().size());
  }

}

