package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;

/**
 * Resources Management Test.
 */
public class ResourcesManagementTest extends BaseTest {

  @RayRemote
  public static Integer echo(Integer number) {
    return number;
  }

  @RayRemote
  public static class Echo {
    public Integer echo(Integer number) {
      return number;
    }
  }

  @Test
  public void testMethods() {
    CallOptions callOptions1 = new CallOptions(ImmutableMap.of("CPU", 4.0, "GPU", 0.0));

    // This is a case that can satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    RayObject<Integer> result1 = Ray.call(ResourcesManagementTest::echo, 100, callOptions1);
    Assert.assertEquals(100, (int) result1.get());

    CallOptions callOptions2 = new CallOptions(ImmutableMap.of("CPU", 4.0, "GPU", 2.0));

    // This is a case that can't satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    final RayObject<Integer> result2 = Ray.call(ResourcesManagementTest::echo, 200, callOptions2);
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result2), 1, 1000);

    Assert.assertEquals(0, waitResult.getReady().size());
    Assert.assertEquals(1, waitResult.getUnready().size());
  }

  @Test
  public void testActors() {

    ActorCreationOptions actorCreationOptions1 =
        new ActorCreationOptions(ImmutableMap.of("CPU", 2.0, "GPU", 0.0));

    // This is a case that can satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    RayActor<Echo> echo1 = Ray.createActor(Echo::new, actorCreationOptions1);
    final RayObject<Integer> result1 = Ray.call(Echo::echo, echo1, 100);
    Assert.assertEquals(100, (int) result1.get());

    // This is a case that can't satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    ActorCreationOptions actorCreationOptions2 =
        new ActorCreationOptions(ImmutableMap.of("CPU", 8.0, "GPU", 0.0));

    RayActor<ResourcesManagementTest.Echo> echo2 =
        Ray.createActor(Echo::new, actorCreationOptions2);
    final RayObject<Integer> result2 = Ray.call(Echo::echo, echo2, 100);
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result2), 1, 1000);

    Assert.assertEquals(0, waitResult.getReady().size());
    Assert.assertEquals(1, waitResult.getUnready().size());
  }

}

