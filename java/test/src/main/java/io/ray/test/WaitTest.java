package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WaitTest extends BaseTest {

  private static String hi() {
    return "hi";
  }

  private static String delayedHi() {
    try {
      Thread.sleep(100 * 1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return "hi";
  }

  private static void testWait() {
    // Call a task in advance to warm up the cluster to avoid being too slow to start workers.
    TestUtils.warmUpCluster();

    ObjectRef<String> obj1 = Ray.task(WaitTest::hi).remote();
    ObjectRef<String> obj2 = Ray.task(WaitTest::delayedHi).remote();

    List<ObjectRef<String>> waitList = ImmutableList.of(obj1, obj2);
    WaitResult<String> waitResult = Ray.wait(waitList, 2, 2 * 1000);

    List<ObjectRef<String>> readyList = waitResult.getReady();

    Assert.assertEquals(1, waitResult.getReady().size());
    Assert.assertEquals(1, waitResult.getUnready().size());
    Assert.assertEquals("hi", readyList.get(0).get());
  }

  @Test
  public void testWaitInDriver() {
    testWait();
  }

  public static Object waitInWorker() {
    testWait();
    return null;
  }

  @Test
  public void testWaitInWorker() {
    ObjectRef<Object> res = Ray.task(WaitTest::waitInWorker).remote();
    res.get();
  }

  @Test
  public void testWaitForEmpty() {
    WaitResult<String> result = Ray.wait(new ArrayList<>());
    Assert.assertTrue(result.getReady().isEmpty());
    Assert.assertTrue(result.getUnready().isEmpty());

    try {
      Ray.wait(null);
      Assert.fail();
    } catch (NullPointerException e) {
      Assert.assertTrue(true);
    }
  }
}
