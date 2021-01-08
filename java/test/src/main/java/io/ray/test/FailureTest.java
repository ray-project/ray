package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFunc0;
import io.ray.api.id.ObjectId;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.exception.RayTaskException;
import io.ray.runtime.exception.RayWorkerException;
import io.ray.runtime.exception.UnreconstructableException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class FailureTest extends BaseTest {

  private static final String EXCEPTION_MESSAGE = "Oops";

  private String oldNumWorkersPerProcess;

  @BeforeClass
  public void setUp() {
    // This is needed by `testGetThrowsQuicklyWhenFoundException`.
    // Set one worker per process. Otherwise, if `badFunc2` and `slowFunc` run in the same
    // process, `sleep` will delay `System.exit`.
    oldNumWorkersPerProcess = System.getProperty("ray.job.num-java-workers-per-process");
    System.setProperty("ray.job.num-java-workers-per-process", "1");
  }

  @AfterClass
  public void tearDown() {
    System.setProperty("ray.job.num-java-workers-per-process", oldNumWorkersPerProcess);
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

  public void testNormalTaskFailure() {
    assertTaskFailedWithRayTaskException(Ray.task(FailureTest::badFunc).remote());
  }

  public void testActorCreationFailure() {
    ActorHandle<BadActor> actor = Ray.actor(BadActor::new, true).remote();
    assertTaskFailedWithRayTaskException(actor.task(BadActor::badMethod).remote());
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

  public void testGetThrowsQuicklyWhenFoundException() {
    List<RayFunc0<Integer>> badFunctions =
        Arrays.asList(FailureTest::badFunc, FailureTest::badFunc2);
    TestUtils.warmUpCluster();
    for (RayFunc0<Integer> badFunc : badFunctions) {
      ObjectRef<Integer> obj1 = Ray.task(badFunc).remote();
      ObjectRef<Integer> obj2 = Ray.task(FailureTest::slowFunc).remote();
      Instant start = Instant.now();
      try {
        Ray.get(Arrays.asList(obj1, obj2));
        Assert.fail("Should throw RayException.");
      } catch (RuntimeException e) {
        Instant end = Instant.now();
        long duration = Duration.between(start, end).toMillis();
        Assert.assertTrue(
            duration < 5000,
            "Should fail quickly. " + "Actual execution time: " + duration + " ms.");
      }
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
}
