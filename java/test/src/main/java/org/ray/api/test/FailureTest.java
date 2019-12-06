package org.ray.api.test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.exception.RayActorException;
import org.ray.api.exception.RayException;
import org.ray.api.exception.RayTaskException;
import org.ray.api.exception.RayWorkerException;
import org.ray.api.function.RayFunc0;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FailureTest extends BaseTest {

  private static final String EXCEPTION_MESSAGE = "Oops";

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

  private static void assertTaskFailedWithRayTaskException(RayObject<?> rayObject) {
    try {
      rayObject.get();
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

  @Test
  public void testNormalTaskFailure() {
    TestUtils.skipTestUnderSingleProcess();
    assertTaskFailedWithRayTaskException(Ray.call(FailureTest::badFunc));
  }

  @Test(groups = {"directCall"})
  public void testActorCreationFailure() {
    TestUtils.skipTestUnderSingleProcess();
    RayActor<BadActor> actor = Ray.createActor(BadActor::new, true);
    assertTaskFailedWithRayTaskException(Ray.call(BadActor::badMethod, actor));
  }

  @Test(groups = {"directCall"})
  public void testActorTaskFailure() {
    TestUtils.skipTestUnderSingleProcess();
    RayActor<BadActor> actor = Ray.createActor(BadActor::new, false);
    assertTaskFailedWithRayTaskException(Ray.call(BadActor::badMethod, actor));
  }

  @Test
  public void testWorkerProcessDying() {
    TestUtils.skipTestUnderSingleProcess();
    try {
      Ray.call(FailureTest::badFunc2).get();
      Assert.fail("This line shouldn't be reached.");
    } catch (RayWorkerException e) {
      // When the worker process dies while executing a task, we should receive an
      // RayWorkerException.
    }
  }

  @Test(groups = {"directCall"})
  public void testActorProcessDying() {
    TestUtils.skipTestUnderSingleProcess();
    // This test case hangs if the worker to worker connection is implemented with grpc.
    // TODO (kfstorm): Should be fixed.
    TestUtils.skipTestIfDirectActorCallEnabled();
    RayActor<BadActor> actor = Ray.createActor(BadActor::new, false);
    try {
      Ray.call(BadActor::badMethod2, actor).get();
      Assert.fail("This line shouldn't be reached.");
    } catch (RayActorException e) {
      // When the actor process dies while executing a task, we should receive an
      // RayActorException.
    }
    try {
      Ray.call(BadActor::badMethod, actor).get();
      Assert.fail("This line shouldn't be reached.");
    } catch (RayActorException e) {
      // When a actor task is submitted to a dead actor, we should also receive an
      // RayActorException.
    }
  }

  @Test
  public void testGetThrowsQuicklyWhenFoundException() {
    TestUtils.skipTestUnderSingleProcess();
    List<RayFunc0<Integer>> badFunctions = Arrays.asList(FailureTest::badFunc,
        FailureTest::badFunc2);
    TestUtils.warmUpCluster();
    for (RayFunc0<Integer> badFunc : badFunctions) {
      RayObject<Integer> obj1 = Ray.call(badFunc);
      RayObject<Integer> obj2 = Ray.call(FailureTest::slowFunc);
      Instant start = Instant.now();
      try {
        Ray.get(Arrays.asList(obj1.getId(), obj2.getId()));
        Assert.fail("Should throw RayException.");
      } catch (RayException e) {
        Instant end = Instant.now();
        long duration = Duration.between(start, end).toMillis();
        Assert.assertTrue(duration < 5000, "Should fail quickly. " +
            "Actual execution time: " + duration + " ms.");
      }
    }
  }
}

