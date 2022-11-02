package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class SetupJavaWorkerTest extends BaseTest {

  private static final String CODE_SEARCH_PATH = System.getProperty("java.class.path");;

  public static class Counter {

    private int value;

    public Counter(int initValue) {
      this.value = initValue;
    }

    public int getValue() {
      return value;
    }
  }

  @BeforeClass
  public void setup() {
    // mock a fake code search path contains whitespace
    String newCodeSearchPathStr = CODE_SEARCH_PATH + ":fakepathStart fakePathEnd";
    System.setProperty("java.class.path", newCodeSearchPathStr);
  }

  @AfterClass
  public void clean() {
    // restore class path
    System.setProperty("java.class.path", CODE_SEARCH_PATH);
  }
  /**
   * raylet will call setup_worker.py with arguments to start java workers after Ray 2.0.0, and
   * setup_worker.py can not start java workers correctly when argumens contian whitespace. This
   * test case is used to test whether the bug is fixed.
   */
  public void testCreateActorFail() {
    ActorHandle<ActorTest.Counter> actor = Ray.actor(ActorTest.Counter::new, 1).remote();
    // throw RayTimeoutException exception if actor is not started correctly by raylet
    Integer value = actor.task(ActorTest.Counter::getValue).remote().get(30 * 1000);
    Assert.assertEquals(Integer.valueOf(1), value);
  }
}
