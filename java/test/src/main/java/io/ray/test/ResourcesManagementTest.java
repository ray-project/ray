package io.ray.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import io.ray.api.options.CallOptions;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Resources Management Test. */
@Test(groups = {"cluster"})
public class ResourcesManagementTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.head-args.0", "--num-cpus=4");
    System.setProperty("ray.head-args.1", "--resources={\"RES-A\":4}");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.head-args.0");
    System.clearProperty("ray.head-args.1");
  }

  public static Integer echo(Integer number) {
    return number;
  }

  public static class Echo {

    public Integer echo(Integer number) {
      return number;
    }
  }

  public void testMethods() {
    // This is a case that can satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    ObjectRef<Integer> result1 =
        Ray.task(ResourcesManagementTest::echo, 100).setResource("CPU", 4.0).remote();
    Assert.assertEquals(100, (int) result1.get());

    // This is a case that can't satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    final ObjectRef<Integer> result2 =
        Ray.task(ResourcesManagementTest::echo, 200).setResource("CPU", 4.0).remote();
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result2), 1, 1000);

    Assert.assertEquals(1, waitResult.getReady().size());
    Assert.assertEquals(0, waitResult.getUnready().size());

    try {
      CallOptions callOptions3 =
          new CallOptions.Builder().setResources(ImmutableMap.of("CPU", 0.0)).build();
      Assert.fail();
    } catch (RuntimeException e) {
      // We should receive a RuntimeException indicates that we should not
      // pass a zero capacity resource.
    }
  }

  public void testActors() {
    // This is a case that can satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    ActorHandle<Echo> echo1 = Ray.actor(Echo::new).setResource("CPU", 2.0).remote();
    final ObjectRef<Integer> result1 = echo1.task(Echo::echo, 100).remote();
    Assert.assertEquals(100, (int) result1.get());

    // This is a case that can't satisfy required resources.
    // The static resources for test are "CPU:4,RES-A:4".
    ActorHandle<Echo> echo2 = Ray.actor(Echo::new).setResource("CPU", 8.0).remote();
    final ObjectRef<Integer> result2 = echo2.task(Echo::echo, 100).remote();
    WaitResult<Integer> waitResult = Ray.wait(ImmutableList.of(result2), 1, 1000);

    Assert.assertEquals(0, waitResult.getReady().size());
    Assert.assertEquals(1, waitResult.getUnready().size());
  }
}
