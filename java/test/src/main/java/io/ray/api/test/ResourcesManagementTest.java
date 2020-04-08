package io.ray.api.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.ray.api.Ray;
import io.ray.api.RayActor;
import io.ray.api.RayObject;
import io.ray.api.TestUtils;
import io.ray.api.WaitResult;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.CallOptions;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Resources Management Test.
 */
public class ResourcesManagementTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.resources", "CPU:4,RES-A:4");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.resources");
  }

  public static Integer echo(Integer number) {
    return number;
  }

  public static class Echo {

    public Integer echo(Integer number) {
      return number;
    }
  }

  @Test
  public void testMethods() {
    TestUtils.skipTestUnderSingleProcess();
    CallOptions callOptions1 =
        new CallOptions.Builder().setResources(ImmutableMap.of("CPU", 4.0)).createCallOptions();

    // This is a case that can satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    RayObject<Integer> result1 = Ray.call(ResourcesManagementTest::echo, 100, callOptions1);
    Assert.assertEquals(100, (int) result1.get());

    CallOptions callOptions2 =
        new CallOptions.Builder().setResources(ImmutableMap.of("CPU", 4.0)).createCallOptions();

    // This is a case that can't satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    final RayObject<Integer> result2 = Ray.call(ResourcesManagementTest::echo, 200, callOptions2);
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result2), 1, 1000);

    Assert.assertEquals(1, waitResult.getReady().size());
    Assert.assertEquals(0, waitResult.getUnready().size());

    try {
      CallOptions callOptions3 =
          new CallOptions.Builder().setResources(ImmutableMap.of("CPU", 0.0)).createCallOptions();
      Assert.fail();
    } catch (RuntimeException e) {
      // We should receive a RuntimeException indicates that we should not
      // pass a zero capacity resource.
    }
  }

  @Test
  public void testActors() {
    TestUtils.skipTestUnderSingleProcess();

    ActorCreationOptions actorCreationOptions1 = new ActorCreationOptions.Builder()
        .setResources(ImmutableMap.of("CPU", 2.0)).createActorCreationOptions();
    // This is a case that can satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    RayActor<Echo> echo1 = Ray.createActor(Echo::new, actorCreationOptions1);
    final RayObject<Integer> result1 = echo1.call(Echo::echo, 100);
    Assert.assertEquals(100, (int) result1.get());

    // This is a case that can't satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    ActorCreationOptions actorCreationOptions2 = new ActorCreationOptions.Builder()
        .setResources(ImmutableMap.of("CPU", 8.0)).createActorCreationOptions();

    RayActor<ResourcesManagementTest.Echo> echo2 =
        Ray.createActor(Echo::new, actorCreationOptions2);
    final RayObject<Integer> result2 = echo2.call(Echo::echo, 100);
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result2), 1, 1000);

    Assert.assertEquals(0, waitResult.getReady().size());
    Assert.assertEquals(1, waitResult.getUnready().size());
  }

}
