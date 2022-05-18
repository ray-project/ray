package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayTaskException;
import io.ray.api.exception.RayWorkerException;
import io.ray.api.exception.UnreconstructableException;
import io.ray.api.function.RayFunc0;
import io.ray.api.id.ObjectId;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class FailureTest extends BaseTest {

  private static final String EXCEPTION_MESSAGE = "Oops";

  @BeforeClass
  public void setUp() {
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
    public SlowActor(ActorHandle<SignalActor> signalActor) {
      if (Ray.getRuntimeContext().wasCurrentActorRestarted()) {
        signalActor.task(SignalActor::waitSignal).remote().get();
      }
    }

    public String ping() {
      return "pong";
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

  public void testActorTaskFastFail() throws IOException, InterruptedException {
    ActorHandle<SignalActor> signalActor = SignalActor.create();
    // NOTE(kfstorm): Currently, `max_task_retries` is always 0 for actors created in Java.
    // Once `max_task_retries` is configurable in Java, we'd better set it to 0 explicitly to show
    // the test scenario.
    ActorHandle<SlowActor> actor =
        Ray.actor(SlowActor::new, signalActor).setMaxRestarts(1).remote();
    actor.task(SlowActor::ping).remote().get();
    actor.kill(/*noRestart=*/ false);

    // Wait for a while so that now the driver knows the actor is in RESTARTING state.
    Thread.sleep(1000);
    // An actor task should fail quickly until the actor is restarted.
    Assert.expectThrows(RayActorException.class, () -> actor.task(SlowActor::ping).remote().get());

    signalActor.task(SignalActor::sendSignal).remote().get();
    // Wait for a while so that now the driver knows the actor is in ALIVE state.
    Thread.sleep(1000);
    // An actor task should succeed.
    actor.task(SlowActor::ping).remote().get();
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
              Ray.put(new RayTaskException(10008, "localhost", "xxx", new RayActorException()))
                  .get();
            });
    Assert.assertEquals(ex1.getCause().getClass(), RayActorException.class);
    RayTaskException ex2 =
        Assert.expectThrows(
            RayTaskException.class,
            () -> {
              Ray.put(new RayTaskException(10008, "localhost", "xxx", new RayWorkerException()))
                  .get();
            });
    Assert.assertEquals(ex2.getCause().getClass(), RayWorkerException.class);

    ObjectId objectId = ObjectId.fromRandom();
    RayTaskException ex3 =
        Assert.expectThrows(
            RayTaskException.class,
            () -> {
              Ray.put(
                      new RayTaskException(
                          10008, "localhost", "xxx", new UnreconstructableException(objectId)))
                  .get();
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
