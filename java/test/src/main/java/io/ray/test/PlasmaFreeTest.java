package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
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
    ObjectRef<String> helloId = Ray.task(PlasmaFreeTest::hello).remote();
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

  @Test(groups = {"cluster"})
  public void testDeleteCreatingTasks() {
    ObjectRef<String> helloId = Ray.task(PlasmaFreeTest::hello).remote();
    Assert.assertEquals("hello", helloId.get());
    Ray.internal().free(ImmutableList.of(helloId.getId()), true, true);

    TaskId taskId = TaskId.fromBytes(Arrays.copyOf(helloId.getId().getBytes(), TaskId.LENGTH));
    final boolean result = TestUtils.waitForCondition(
        () -> !TestUtils.getRuntime().getGcsClient()
            .rayletTaskExistsInGcs(taskId), 50);
    Assert.assertTrue(result);
  }

}
