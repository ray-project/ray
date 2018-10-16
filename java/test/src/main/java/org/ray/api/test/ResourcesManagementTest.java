package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayResources;
import org.ray.api.WaitResult;
import org.ray.api.annotation.RayRemote;

/**
 * Resources Management Test.
 */
@RunWith(MyRunner.class)
public class ResourcesManagementTest {

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
    RayResources resource1 = new RayResources();
    resource1.add("CPU", 4.0);
    resource1.add("GPU", 0.0);

    // This is a case that can satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    RayObject<Integer> result1 = Ray.call(ResourcesManagementTest::echo, 100, resource1);
    Assert.assertEquals(100, (int) result1.get());

    RayResources resource2 = new RayResources();
    resource2.add("CPU", 4.0);
    resource2.add("GPU", 2.0);

    // This is a case that can't satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    final RayObject<Integer> result2 = Ray.call(ResourcesManagementTest::echo, 200, resource2);
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result2), 1, 1000);

    Assert.assertEquals(0, waitResult.getReady().size());
    Assert.assertEquals(1, waitResult.getUnready().size());
  }

  @Test
  public void testActors() {
    RayResources resource1 = new RayResources();
    resource1.add("CPU", 2.0);
    resource1.add("GPU", 0.0);

    // This is a case that can satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    RayActor<Echo> echo1 = Ray.createActor(Echo::new, resource1);
    final RayObject<Integer> result1 = Ray.call(Echo::echo, echo1, 100);
    Assert.assertEquals(100, (int) result1.get());

    // This is a case that can't satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    RayResources resource2 = new RayResources();
    resource2.add("CPU", 8.0);
    resource2.add("GPU", 0.0);

    RayActor<ResourcesManagementTest.Echo> echo2 = Ray.createActor(Echo::new, resource2);
    final RayObject<Integer> result2 = Ray.call(Echo::echo, echo2, 100);
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result2), 1, 1000);

    Assert.assertEquals(0, waitResult.getReady().size());
    Assert.assertEquals(1, waitResult.getUnready().size());
  }

  @Test
  public void testActorAndMemberMethods() {
    // Note(qwang): This case depends on  the following line.
    // https://github.com/ray-project/ray/blob/master/java/test/src/main/java/org/ray/api/test/TestListener.java#L13
    // If we change the static resources configuration item, this case may not pass.
    // Then we should change this case too.
    RayResources resource = new RayResources();
    resource.add("RES-A", 4.0);

    RayActor<Echo> echo3 = Ray.createActor(Echo::new, resource);
    Assert.assertEquals(100, (int) Ray.call(Echo::echo, echo3, 100).get());
    Assert.assertEquals(100, (int) Ray.call(Echo::echo, echo3, 100).get());

    // This case shows that if we specify a required resource for an actor
    // task(not actor creation task), it means this task need an additional resource.
    final RayObject<Integer> result = Ray.call(Echo::echo, echo3, 100, resource);
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result), 1, 1000);
    Assert.assertEquals(0, waitResult.getReady().size());
    Assert.assertEquals(1, waitResult.getUnready().size());
  }
}

