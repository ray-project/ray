package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import org.testng.Assert;
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

  public void testSpecifyZeroCPU() {
    ActorHandle<Echo> echo = Ray.actor(Echo::new).setResource("CPU", 0.0).remote();
    final ObjectRef<Integer> result1 = echo.task(Echo::echo, 100).remote();
    Assert.assertEquals(100, (int) result1.get());
  }

  public void testSpecifyZeroCustomResource() {
    ActorHandle<Echo> echo = Ray.actor(Echo::new).setResource("A", 0.0).remote();
    final ObjectRef<Integer> result1 = echo.task(Echo::echo, 100).remote();
    Assert.assertEquals(100, (int) result1.get());
  }
}
