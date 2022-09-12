package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class SetupJavaWorkerTest {

  public static class Counter {

    private int value;

    public Counter(int initValue) {
      this.value = initValue;
    }

    public int getValue() {
      return value;
    }
  }

  /**
   * raylet will call setup_worker.py with arguments to start java workers after Ray 2.0.0, and
   * setup_worker.py can not start java workers correctly when argumens contian space. This test
   * case is used to test whether the bug is fixed.
   */
  public void testCreateActorFail()
      throws InterruptedException, NoSuchFieldException, IllegalAccessException {
    String oldCodeSearchPathStr = System.getProperty("java.class.path");
    // mock a fake code search path contains space
    String newCodeSearchPathStr = oldCodeSearchPathStr + ":fakepathStart fakePathEnd";
    System.setProperty("java.class.path", newCodeSearchPathStr);
    Ray.init();
    ActorHandle<ActorTest.Counter> actor = Ray.actor(ActorTest.Counter::new, 1).remote();
    // throw RayTimeoutException exception if actor is not started correctly by raylet
    Integer value = actor.task(ActorTest.Counter::getValue).remote().get(3 * 1000);
    Assert.assertEquals(Integer.valueOf(1), value);
    System.setProperty("java.class.path", oldCodeSearchPathStr);
    Ray.shutdown();
  }
}
