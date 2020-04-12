package io.ray.api.test;

import io.ray.api.Ray;
import io.ray.api.RayActor;
import io.ray.api.RayObject;
import io.ray.api.TestUtils;
import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayException;
import io.ray.api.exception.RayTaskException;
import io.ray.api.exception.RayWorkerException;
import io.ray.api.function.RayFunc0;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class FailureTest extends BaseTest {

  private static final String EXCEPTION_MESSAGE = "Oops";

  @BeforeClass
  public void setUp() {
    // This is needed by `testGetThrowsQuicklyWhenFoundException`.
    // Set one worker per process. Otherwise, if `badFunc2` and `slowFunc` run in the same
    // process, `sleep` will delay `System.exit`.
    System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.raylet.config.num_workers_per_process_java");
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

  @Test
  public void testActorCreationFailure() {
    TestUtils.skipTestUnderSingleProcess();
    RayActor<BadActor> actor = Ray.createActor(BadActor::new, true);
    assertTaskFailedWithRayTaskException(actor.call(BadActor::badMethod));
  }

  @Test
  public void testActorTaskFailure() {
    TestUtils.skipTestUnderSingleProcess();
    RayActor<BadActor> actor = Ray.createActor(BadActor::new, false);
    assertTaskFailedWithRayTaskException(actor.call(BadActor::badMethod));
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

  @Test
  public void testActorProcessDying() {
    TestUtils.skipTestUnderSingleProcess();
    RayActor<BadActor> actor = Ray.createActor(BadActor::new, false);
    try {
      actor.call(BadActor::badMethod2).get();
      Assert.fail("This line shouldn't be reached.");
    } catch (RayActorException e) {
      // When the actor process dies while executing a task, we should receive an
      // RayActorException.
    }
    try {
      actor.call(BadActor::badMethod).get();
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
        Ray.get(Arrays.asList(obj1, obj2));
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

