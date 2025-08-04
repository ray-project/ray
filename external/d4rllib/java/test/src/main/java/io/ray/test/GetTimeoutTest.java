package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.exception.RayTimeoutException;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class GetTimeoutTest extends BaseTest {

  public static int squareDelay(int x) {
    try {
      Thread.sleep(5 * 1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return x * x;
  }

  public static class Counter {

    private int value;

    public Counter(int initValue) {
      this.value = initValue;
    }

    public int getValueDelay() {
      try {
        Thread.sleep(5 * 1000);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return value;
    }

    public int getValue() {
      return value;
    }
  }

  public void testActorTaskGetTimeout() {
    ActorHandle<Counter> actor = Ray.actor(Counter::new, 1).remote();
    Assert.assertEquals(Integer.valueOf(1), actor.task(Counter::getValueDelay).remote().get());
    Assert.assertEquals(Integer.valueOf(1), actor.task(Counter::getValueDelay).remote().get(10000));

    long startTime = System.currentTimeMillis();
    long timeout = 2000;
    Assert.assertThrows(
        RayTimeoutException.class,
        () -> Ray.get(actor.task(Counter::getValueDelay).remote(), timeout));
    long waitTime = System.currentTimeMillis() - startTime;
    Assert.assertTrue(Math.abs(waitTime - timeout) < 1500);
  }

  public void testNormalTaskGetTimeout() {
    List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      objectRefList.add(Ray.task(GetTimeoutTest::squareDelay, i).remote());
    }
    long startTime = System.currentTimeMillis();
    long timeout = 2000;
    Assert.assertThrows(RayTimeoutException.class, () -> Ray.get(objectRefList, timeout));
    long waitTime = System.currentTimeMillis() - startTime;
    Assert.assertTrue(Math.abs(waitTime - timeout) < 1500);
    Assert.assertEquals(Ray.get(objectRefList, 10000).toArray(), new int[] {0, 1, 4, 9});
    Assert.assertEquals(Ray.get(objectRefList).toArray(), new int[] {0, 1, 4, 9});
  }

  public void testObjectGetTimeout() {
    int y = 1;
    ObjectRef<Integer> objectRef = Ray.put(y);
    Assert.assertTrue(Ray.get(objectRef) == 1);
    Assert.assertTrue(Ray.get(objectRef, 2000) == 1);
  }
}
