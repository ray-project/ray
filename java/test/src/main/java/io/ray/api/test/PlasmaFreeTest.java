package io.ray.api.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.Ray;
import io.ray.api.RayObject;
import io.ray.api.TestUtils;
import io.ray.api.id.TaskId;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PlasmaFreeTest extends BaseTest {

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
        !TestUtils.getRuntime().getObjectStore()
          .wait(ImmutableList.of(helloId.getId()), 1, 0).get(0), 50);
    if (TestUtils.isSingleProcessMode()) {
      Assert.assertTrue(result);
    } else {
      // The object will not be deleted under cluster mode.
      Assert.assertFalse(result);
    }
  }

  @Test
  public void testDeleteCreatingTasks() {
    TestUtils.skipTestUnderSingleProcess();
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    Assert.assertEquals("hello", helloId.get());
    Ray.internal().free(ImmutableList.of(helloId.getId()), true, true);

    TaskId taskId = TaskId.fromBytes(Arrays.copyOf(helloId.getId().getBytes(), TaskId.LENGTH));
    final boolean result = TestUtils.waitForCondition(
        () -> !TestUtils.getRuntime().getGcsClient()
            .rayletTaskExistsInGcs(taskId), 50);
    Assert.assertTrue(result);
  }

}
