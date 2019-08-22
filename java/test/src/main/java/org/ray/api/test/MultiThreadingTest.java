package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.WaitResult;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ActorId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiThreadingTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadingTest.class);

  private static final int LOOP_COUNTER = 100;
  private static final int NUM_THREADS = 20;

  @RayRemote
  public static Integer echo(int num) {
    return num;
  }

  @RayRemote
  public static class Echo {

    @RayRemote
    public Integer echo(int num) {
      return num;
    }
  }

  @RayRemote
  public static class ActorIdTester {

    private final ActorId actorId;

    public ActorIdTester() {
      actorId = Ray.getRuntimeContext().getCurrentActorId();
      Assert.assertNotEquals(actorId, ActorId.NIL);
    }

    @RayRemote
    public ActorId getCurrentActorId() {
      final ActorId[] result = new ActorId[1];
      Thread thread = new Thread(() -> {
        result[0] = Ray.getRuntimeContext().getCurrentActorId();
      });
      thread.start();
      try {
        thread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      Assert.assertEquals(result[0], actorId);
      return result[0];
    }
  }

  public static String testMultiThreading() {
    Random random = new Random();
    // Test calling normal functions.
    runTestCaseInMultipleThreads(() -> {
      int arg = random.nextInt();
      RayObject<Integer> obj = Ray.call(MultiThreadingTest::echo, arg);
      Assert.assertEquals(arg, (int) obj.get());
    }, LOOP_COUNTER);

    // Test calling actors.
    RayActor<Echo> echoActor = Ray.createActor(Echo::new);
    runTestCaseInMultipleThreads(() -> {
      int arg = random.nextInt();
      RayObject<Integer> obj = Ray.call(Echo::echo, echoActor, arg);
      Assert.assertEquals(arg, (int) obj.get());
    }, LOOP_COUNTER);

    // Test creating multi actors
    runTestCaseInMultipleThreads(() -> {
      int arg = random.nextInt();
      RayActor<Echo> echoActor1 = Ray.createActor(Echo::new);
      try {
        // Sleep a while to test the case that another actor is created before submitting
        // tasks to this actor.
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        LOGGER.warn("Got exception while sleeping.", e);
      }
      RayObject<Integer> obj = Ray.call(Echo::echo, echoActor1, arg);
      Assert.assertEquals(arg, (int) obj.get());
    }, 1);

    // Test put and get.
    runTestCaseInMultipleThreads(() -> {
      int arg = random.nextInt();
      RayObject<Integer> obj = Ray.put(arg);
      Assert.assertEquals(arg, (int) Ray.get(obj.getId()));
    }, LOOP_COUNTER);

    TestUtils.warmUpCluster();
    // Test wait for one object in multi threads.
    RayObject<Integer> obj = Ray.call(MultiThreadingTest::echo, 100);
    runTestCaseInMultipleThreads(() -> {
      WaitResult<Integer> result = Ray.wait(ImmutableList.of(obj), 1, 1000);
      Assert.assertEquals(1, result.getReady().size());
    }, 1);

    return "ok";
  }

  @Test
  public void testInDriver() {
    testMultiThreading();
  }

  @Test
  public void testInWorker() {
    // Single-process mode doesn't have real workers.
    TestUtils.skipTestUnderSingleProcess();
    RayObject<String> obj = Ray.call(MultiThreadingTest::testMultiThreading);
    Assert.assertEquals("ok", obj.get());
  }

  @Test
  public void testGetCurrentActorId() {
    TestUtils.skipTestUnderSingleProcess();
    RayActor<ActorIdTester> actorIdTester = Ray.createActor(ActorIdTester::new);
    ActorId actorId = Ray.call(ActorIdTester::getCurrentActorId, actorIdTester).get();
    Assert.assertEquals(actorId, actorIdTester.getId());
  }

  private static void runTestCaseInMultipleThreads(Runnable testCase, int numRepeats) {
    ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);

    try {
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < NUM_THREADS; i++) {
        Callable<String> task = () -> {
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
