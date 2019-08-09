package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.TaskId;
import org.ray.runtime.AbstractRayRuntime;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PlasmaFreeTest extends BaseTest {

  @RayRemote
  private static String hello() {
    return "hello";
  }

  @Test
  public void testDeleteObjects() {
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    String helloString = helloId.get();
    Assert.assertEquals("hello", helloString);
    Ray.internal().free(ImmutableList.of(helloId.getId()), true, false);

    final boolean result = TestUtils.waitForCondition(() ->
        ((AbstractRayRuntime) Ray.internal()).getObjectStore()
            .getRaw(ImmutableList.of(helloId.getId()), 0).get(0) == null, 50);
    Assert.assertTrue(result);
  }

  @Test
  public void testDeleteCreatingTasks() {
    TestUtils.skipTestUnderSingleProcess();
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    Assert.assertEquals("hello", helloId.get());
    Ray.internal().free(ImmutableList.of(helloId.getId()), true, true);

    TaskId taskId = TaskId.fromBytes(Arrays.copyOf(helloId.getId().getBytes(), TaskId.LENGTH));
    final boolean result = TestUtils.waitForCondition(
        () -> !(((AbstractRayRuntime) Ray.internal()).getGcsClient())
            .rayletTaskExistsInGcs(taskId), 50);
    Assert.assertTrue(result);
  }

}
