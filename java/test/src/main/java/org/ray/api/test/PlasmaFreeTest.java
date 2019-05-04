package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.util.UniqueIdUtil;
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

    final boolean result = TestUtils.waitForCondition(() -> !((AbstractRayRuntime) Ray.internal())
        .getObjectStoreProxy().get(helloId.getId(), 0).exists, 50);
    Assert.assertTrue(result);
  }

  @Test
  public void testDeleteCreatingTasks() {
    TestUtils.skipTestUnderSingleProcess();
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    Assert.assertEquals("hello", helloId.get());
    Ray.internal().free(ImmutableList.of(helloId.getId()), true, true);

    final boolean result = TestUtils.waitForCondition(
        () ->  !(((AbstractRayRuntime)Ray.internal()).getGcsClient())
          .rayletTaskExistsInGcs(UniqueIdUtil.computeTaskId(helloId.getId())), 50);
    Assert.assertTrue(result);
  }

}
