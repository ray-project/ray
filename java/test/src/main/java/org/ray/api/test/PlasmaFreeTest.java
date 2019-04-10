package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.function.Supplier;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.RayNativeRuntime;
import org.ray.runtime.util.UniqueIdUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PlasmaFreeTest extends BaseTest {

  private static final int WAIT_INTERVAL = 5;
  private static final int WAIT_TIMEOUT_LIMIT_MS = 50;

  @RayRemote
  private static String hello() {
    return "hello";
  }

  @Test
  public void test() throws InterruptedException {
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    String helloString = helloId.get();
    Assert.assertEquals("hello", helloString);
    Ray.internal().free(ImmutableList.of(helloId.getId()), true, false);

    final boolean result = waitForCondition(() -> {
      return !((AbstractRayRuntime) Ray.internal())
          .getObjectStoreProxy().get(helloId.getId(), 0).exists;
    }, WAIT_INTERVAL, WAIT_TIMEOUT_LIMIT_MS);

    Assert.assertTrue(result);
  }

  @Test
  public void testDeleteCreatingTasks() throws InterruptedException {
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    Assert.assertEquals("hello", helloId.get());
    Ray.internal().free(ImmutableList.of(helloId.getId()), true, true);

    final boolean result = waitForCondition(() -> {
      return !((RayNativeRuntime) Ray.internal())
          .rayletTaskExistsInGcs(UniqueIdUtil.computeTaskId(helloId.getId()));
    }, WAIT_INTERVAL, WAIT_TIMEOUT_LIMIT_MS);

    Assert.assertTrue(result);
  }

  private boolean waitForCondition(Supplier<Boolean> condition,
                                   int interval, int timeout) throws InterruptedException {
    boolean result = false;
    int waitTime = 0;
    while (true) {
      result = condition.get();
      if (result) {
        break;
      }

      waitTime += interval;
      if (waitTime > timeout) {
        break;
      }
      java.util.concurrent.TimeUnit.MILLISECONDS.sleep(interval);
    }
    return result;
  }

}
