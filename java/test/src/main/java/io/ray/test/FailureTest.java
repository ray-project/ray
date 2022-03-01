package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFunc0;
import io.ray.api.id.ObjectId;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.exception.RayException;
import io.ray.runtime.exception.RayTaskException;
import io.ray.runtime.exception.RayWorkerException;
import io.ray.runtime.exception.UnreconstructableException;
import io.ray.runtime.util.SystemUtil;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class FailureTest extends BaseTest {

  private static final String EXCEPTION_MESSAGE = "Oops";

  @BeforeClass
  public void setUp() {
    // This is needed by `testGetThrowsQuicklyWhenFoundException`.
    // Set one worker per process. Otherwise, if `badFunc2` and `slowFunc` run in the same
    // process, `sleep` will delay `System.exit`.
    System.setProperty("ray.job.num-java-workers-per-process", "1");
    System.setProperty("ray.raylet.startup-token", "0");
  }

  public static int badFunc() {
    throw new RuntimeException(EXCEPTION_MESSAGE);
  }

  public static int badFunc2() {
    System.exit(-1);
    return 0;
  }

  public static int slowFunc() {
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return 0;
  }

  public static int echo(int obj) {
    return obj;
  }

  public static class BadActor {

    public BadActor(boolean failOnCreation) {
      if (failOnCreation) {
        throw new RuntimeException(EXCEPTION_MESSAGE);
      }
    }

    public int badMethod() {
      throw new RuntimeException(EXCEPTION_MESSAGE);
    }

    public int badMethod2() {
      System.exit(-1);
      return 0;
    }
  }

  public static class SlowActor {
    public SlowActor() {
      try {
        // This is to slow down the restarting process and make the test case more stable.
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public int getPid() {
      return SystemUtil.pid();
    }
  }

  private static void assertTaskFailedWithRayTaskException(ObjectRef<?> objectRef) {
    try {
      objectRef.get();
      Assert.fail("Task didn't fail.");
    } catch (RayTaskException e) {
      Throwable rootCause = e.getCause();
      while (rootCause.getCause() != null) {
        rootCause = rootCause.getCause();
      }
      Assert.assertTrue(rootCause instanceof RuntimeException);
      Assert.assertEquals(rootCause.getMessage(), EXCEPTION_MESSAGE);
    }
  }

  private static void assertTaskFailedWithRayActorException(ObjectRef<?> objectRef) {
    try {
      objectRef.get();
      Assert.fail("Task didn't fail.");
    } catch (RayActorException e) {
      Throwable rootCause = e;
      while (rootCause.getCause() != null) {
        rootCause = rootCause.getCause();
      }
      Assert.assertTrue(rootCause instanceof RuntimeException);
      Assert.assertTrue(rootCause.getMessage().contains(EXCEPTION_MESSAGE));
    }
  }

  public void testNormalTaskFailure() {
    assertTaskFailedWithRayTaskException(Ray.task(FailureTest::badFunc).remote());
  }

  public void testActorCreationFailure() {
    ActorHandle<BadActor> actor = Ray.actor(BadActor::new, true).remote();
    assertTaskFailedWithRayActorException(actor.task(BadActor::badMethod).remote());
  }

  public void testActorTaskFailure() {
    ActorHandle<BadActor> actor = Ray.actor(BadActor::new, false).remote();
    assertTaskFailedWithRayTaskException(actor.task(BadActor::badMethod).remote());
  }

  public void testWorkerProcessDying() {
    try {
      Ray.task(FailureTest::badFunc2).remote().get();
      Assert.fail("This line shouldn't be reached.");
    } catch (RayWorkerException e) {
      // When the worker process dies while executing a task, we should receive an
      // RayWorkerException.
    }
  }

  public void testActorProcessDying() {
    ActorHandle<BadActor> actor = Ray.actor(BadActor::new, false).remote();
    try {
      actor.task(BadActor::badMethod2).remote().get();
      Assert.fail("This line shouldn't be reached.");
    } catch (RayActorException e) {
      // When the actor process dies while executing a task, we should receive an
      // RayActorException.
      Assert.assertEquals(e.actorId, actor.getId());
    }
    try {
      actor.task(BadActor::badMethod).remote().get();
      Assert.fail("This line shouldn't be reached.");
    } catch (RayActorException e) {
      // When a actor task is submitted to a dead actor, we should also receive an
      // RayActorException.
    }
  }

  public void testActorTaskFastFail() throws IOException {
    ActorHandle<SlowActor> actor =
        Ray.actor(SlowActor::new).setMaxRestarts(1).setEnableTaskFastFail(true).remote();
    int pid = actor.task(SlowActor::getPid).remote().get();
    Runtime.getRuntime().exec("kill -9 " + pid);

    // Send tasks until the caller finds out that the actor is unavailable.
    boolean[] exceptionOccurred = new boolean[] {false};
    while (!exceptionOccurred[0]) {
      // Make sure the task execution finishes or the exception throws quickly.
      TestUtils.executeWithinTime(
          () -> {
            try {
              actor.task(SlowActor::getPid).remote().get();
            } catch (RayActorException e) {
              exceptionOccurred[0] = true;
            }
          },
          500);
    }

    // The actor is still restarting. Send more tasks and all of them should fail quickly
    // until the actor is restarted.
    int failedCount = 0;
    while (true) {
      ObjectRef<Integer> newPidObject = actor.task(SlowActor::getPid).remote();
      int newPid = 0;
      long startTime = System.currentTimeMillis();
      try {
        newPid = newPidObject.get();
      } catch (RayException e) {
        failedCount++;
      }
      if (newPid != 0) {
        Assert.assertNotEquals(pid, newPid);
        break;
      } else {
        long endTime = System.currentTimeMillis();
        Assert.assertTrue(endTime - startTime <= 500);
      }
    }
    Assert.assertTrue(failedCount > 0);
  }

  public void testGetThrowsQuicklyWhenFoundException() {
    List<RayFunc0<Integer>> badFunctions =
        Arrays.asList(FailureTest::badFunc, FailureTest::badFunc2);
    TestUtils.warmUpCluster();
    for (RayFunc0<Integer> badFunc : badFunctions) {
      ObjectRef<Integer> obj1 = Ray.task(badFunc).remote();
      ObjectRef<Integer> obj2 = Ray.task(FailureTest::slowFunc).remote();
      TestUtils.executeWithinTime(
          () ->
              Assert.expectThrows(RuntimeException.class, () -> Ray.get(Arrays.asList(obj1, obj2))),
          5000);
    }
  }

  public void testExceptionSerialization() {
    RayTaskException ex1 =
        Assert.expectThrows(
            RayTaskException.class,
            () -> {
              Ray.put(new RayTaskException("xxx", new RayActorException())).get();
            });
    Assert.assertEquals(ex1.getCause().getClass(), RayActorException.class);
    RayTaskException ex2 =
        Assert.expectThrows(
            RayTaskException.class,
            () -> {
              Ray.put(new RayTaskException("xxx", new RayWorkerException())).get();
            });
    Assert.assertEquals(ex2.getCause().getClass(), RayWorkerException.class);

    ObjectId objectId = ObjectId.fromRandom();
    RayTaskException ex3 =
        Assert.expectThrows(
            RayTaskException.class,
            () -> {
              Ray.put(new RayTaskException("xxx", new UnreconstructableException(objectId))).get();
            });
    Assert.assertEquals(ex3.getCause().getClass(), UnreconstructableException.class);
    Assert.assertEquals(((UnreconstructableException) ex3.getCause()).objectId, objectId);
  }

  public void testTaskChainWithException() {
    ObjectRef<Integer> obj1 = Ray.task(FailureTest::badFunc).remote();
    ObjectRef<Integer> obj2 = Ray.task(FailureTest::echo, obj1).remote();
    RayTaskException ex = Assert.expectThrows(RayTaskException.class, () -> Ray.get(obj2));
    Assert.assertTrue(ex.getCause() instanceof RayTaskException);
    RayTaskException ex2 = (RayTaskException) ex.getCause();
    Assert.assertTrue(ex2.getCause() instanceof RuntimeException);
    RuntimeException ex3 = (RuntimeException) ex2.getCause();
    Assert.assertEquals(EXCEPTION_MESSAGE, ex3.getMessage());
  }
}
