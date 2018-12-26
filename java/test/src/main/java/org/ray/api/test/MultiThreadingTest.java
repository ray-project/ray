package org.ray.api.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;


public class MultiThreadingTest extends BaseTest {

  private static final int LOOP_COUNTER = 1000;
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

  public static String testMultiThreading() {
    Random random = new Random();
    // Test calling normal functions.
    runTestCaseInMultipleThreads(() -> {
      int arg = random.nextInt();
      RayObject<Integer> obj = Ray.call(MultiThreadingTest::echo, arg);
      Assert.assertEquals(arg, (int) obj.get());
    });
    // Test calling actors.
    RayActor<Echo> echoActor = Ray.createActor(Echo::new);
    runTestCaseInMultipleThreads(() -> {
      int arg = random.nextInt();
      RayObject<Integer> obj = Ray.call(Echo::echo, echoActor, arg);
      Assert.assertEquals(arg, (int) obj.get());
    });
    // Test put and get.
    runTestCaseInMultipleThreads(() -> {
      int arg = random.nextInt();
      RayObject<Integer> obj = Ray.put(arg);
      Assert.assertEquals(arg, (int) Ray.get(obj.getId()));
    });
    return "ok";
  }

  @Test
  public void testInDriver() {
    testMultiThreading();
  }

  @Test
  public void testInWorker() {
    RayObject<String> obj = Ray.call(MultiThreadingTest::testMultiThreading);
    Assert.assertEquals("ok", obj.get());
  }

  private static void runTestCaseInMultipleThreads(Runnable testCase) {
    ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);

    try {
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < NUM_THREADS; i++) {
        Callable<String> task = () -> {
          for (int j = 0; j < LOOP_COUNTER; j++) {
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
