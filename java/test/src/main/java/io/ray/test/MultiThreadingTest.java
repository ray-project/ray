package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import io.ray.api.id.ActorId;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class MultiThreadingTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadingTest.class);

  private static final int LOOP_COUNTER = 100;
  private static final int NUM_THREADS = 20;

  static Integer echo(int num) {
    return num;
  }

  public static class Echo {

    public Integer echo(int num) {
      return num;
    }
  }

  public static class ActorIdTester {

    private final ActorId actorId;

    public ActorIdTester() {
      actorId = Ray.getRuntimeContext().getCurrentActorId();
      Assert.assertNotEquals(actorId, ActorId.NIL);
    }

    public ActorId getCurrentActorId() throws Exception {
      final Object[] result = new Object[1];
      Thread thread =
          new Thread(
              () -> {
                try {
                  result[0] = Ray.getRuntimeContext().getCurrentActorId();
                } catch (Exception e) {
                  result[0] = e;
                }
              });
      thread.start();
      thread.join();
      if (result[0] instanceof Exception) {
        throw (Exception) result[0];
      }
      Assert.assertEquals(result[0], actorId);
      return (ActorId) result[0];
    }
  }

  static String testMultiThreading() {
    Random random = new Random();
    // Test calling normal functions.
    runTestCaseInMultipleThreads(
        () -> {
          int arg = random.nextInt();
          ObjectRef<Integer> obj = Ray.task(MultiThreadingTest::echo, arg).remote();
          Assert.assertEquals(arg, (int) obj.get());
        },
        LOOP_COUNTER);

    // Test calling actors.
    ActorHandle<Echo> echoActor = Ray.actor(Echo::new).remote();
    runTestCaseInMultipleThreads(
        () -> {
          int arg = random.nextInt();
          ObjectRef<Integer> obj = echoActor.task(Echo::echo, arg).remote();
          Assert.assertEquals(arg, (int) obj.get());
        },
        LOOP_COUNTER);

    // Test creating multi actors
    runTestCaseInMultipleThreads(
        () -> {
          int arg = random.nextInt();
          ActorHandle<Echo> echoActor1 = Ray.actor(Echo::new).remote();
          try {
            // Sleep a while to test the case that another actor is created before submitting
            // tasks to this actor.
            TimeUnit.MILLISECONDS.sleep(10);
          } catch (InterruptedException e) {
            LOGGER.warn("Got exception while sleeping.", e);
          }
          ObjectRef<Integer> obj = echoActor1.task(Echo::echo, arg).remote();
          Assert.assertEquals(arg, (int) obj.get());
        },
        1);

    // Test put and get.
    runTestCaseInMultipleThreads(
        () -> {
          int arg = random.nextInt();
          ObjectRef<Integer> obj = Ray.put(arg);
          Assert.assertEquals(arg, (int) obj.get());
        },
        LOOP_COUNTER);

    TestUtils.warmUpCluster();
    // Test wait for one object in multi threads.
    ObjectRef<Integer> obj = Ray.task(MultiThreadingTest::echo, 100).remote();
    runTestCaseInMultipleThreads(
        () -> {
          WaitResult<Integer> result = Ray.wait(ImmutableList.of(obj), 1, 1000);
          Assert.assertEquals(1, result.getReady().size());
        },
        1);

    return "ok";
  }

  public void testInDriver() {
    testMultiThreading();
  }

  // Local mode doesn't have real workers.
  @Test(groups = {"cluster"})
  public void testInWorker() {
    ObjectRef<String> obj = Ray.task(MultiThreadingTest::testMultiThreading).remote();
    Assert.assertEquals("ok", obj.get());
  }

  /// SINGLE_PROCESS mode doesn't support this API.
  @Test(groups = {"cluster"})
  public void testGetCurrentActorId() {
    ActorHandle<ActorIdTester> actorIdTester = Ray.actor(ActorIdTester::new).remote();
    ActorId actorId = actorIdTester.task(ActorIdTester::getCurrentActorId).remote().get();
    Assert.assertEquals(actorId, actorIdTester.getId());
  }

  /** Call this method each time to avoid hitting the cache in {@link ObjectRef#get()}. */
  static Runnable[] generateRunnables() {
    final ObjectRef<Integer> fooObject = Ray.put(1);
    final ActorHandle<Echo> fooActor = Ray.actor(Echo::new).remote();
    return new Runnable[] {
      () -> Ray.put(1),
      () -> Ray.get(ImmutableList.of(fooObject)),
      fooObject::get,
      () -> Ray.wait(ImmutableList.of(fooObject)),
      Ray::getRuntimeContext,
      () -> Ray.task(MultiThreadingTest::echo, 1).remote(),
      () -> Ray.actor(Echo::new).remote(),
      () -> fooActor.task(Echo::echo, 1).remote(),
    };
  }

  private static void runTestCaseInMultipleThreads(Runnable testCase, int numRepeats) {
    ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);

    try {
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < NUM_THREADS; i++) {
        Callable<String> task =
            () -> {
              for (int j = 0; j < numRepeats; j++) {
                TimeUnit.MILLISECONDS.sleep(1);
                testCase.run();
              }
              return "ok";
            };
        futures.add(service.submit(task));
      }
      for (Future<String> future : futures) {
        try {
          Assert.assertEquals(future.get(), "ok");
        } catch (Exception e) {
          throw new RuntimeException("Test case failed.", e);
        }
      }
    } finally {
      service.shutdown();
    }
  }
}
