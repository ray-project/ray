package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.annotation.RayRemote;

public class WaitTest extends BaseTest {

  @RayRemote
  private static String hi() {
    return "hi";
  }

  @RayRemote
  private static String delayedHi() {
    try {
      Thread.sleep(100 * 1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return "hi";
  }

  private static void testWait() {
    RayObject<String> obj1 = Ray.call(WaitTest::hi);
    RayObject<String> obj2 = Ray.call(WaitTest::delayedHi);

    List<RayObject<String>> waitList = ImmutableList.of(obj1, obj2);
    WaitResult<String> waitResult = Ray.wait(waitList, 2, 2 * 1000);

    List<RayObject<String>> readyList = waitResult.getReady();

    Assert.assertEquals(1, waitResult.getReady().size());
    Assert.assertEquals(1, waitResult.getUnready().size());
    Assert.assertEquals("hi", readyList.get(0).get());
  }

  @Test
  public void testWaitInDriver() {
    testWait();
  }

  @RayRemote
  public static Object waitInWorker() {
    testWait();
    return null;
  }

  @Test
  public void testWaitInWorker() {
    RayObject<Object> res = Ray.call(WaitTest::waitInWorker);
    res.get();
  }
}
