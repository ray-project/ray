package io.ray.api.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.Ray;
import io.ray.api.RayActor;
import io.ray.api.RayObject;
import io.ray.api.TestUtils;
import io.ray.api.WaitResult;
import io.ray.api.exception.RayException;
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
      Thread thread = new Thread(Ray.wrapRunnable(() -> {
        try {
          result[0] = Ray.getRuntimeContext().getCurrentActorId();
        } catch (Exception e) {
          result[0] = e;
        }
      }));
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
    runTestCaseInMultipleThreads(() -> {
      int arg = random.nextInt();
      RayObject<Integer> obj = Ray.call(MultiThreadingTest::echo, arg);
      Assert.assertEquals(arg, (int) obj.get());
    }, LOOP_COUNTER);

    // Test calling actors.
    RayActor<Echo> echoActor = Ray.createActor(Echo::new);
    runTestCaseInMultipleThreads(() -> {
      int arg = random.nextInt();
      RayObject<Integer> obj = echoActor.call(Echo::echo, arg);
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
      RayObject<Integer> obj = echoActor1.call(Echo::echo, arg);
      Assert.assertEquals(arg, (int) obj.get());
    }, 1);

    // Test put and get.
    runTestCaseInMultipleThreads(() -> {
      int arg = random.nextInt();
      RayObject<Integer> obj = Ray.put(arg);
      Assert.assertEquals(arg, (int) obj.get());
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

  public void testInDriver() {
    testMultiThreading();
  }

  public void testInWorker() {
    // Single-process mode doesn't have real workers.
    TestUtils.skipTestUnderSingleProcess();
    RayObject<String> obj = Ray.call(MultiThreadingTest::testMultiThreading);
    Assert.assertEquals("ok", obj.get());
  }

  public void testGetCurrentActorId() {
    TestUtils.skipTestUnderSingleProcess();
    RayActor<ActorIdTester> actorIdTester = Ray.createActor(ActorIdTester::new);
    ActorId actorId = actorIdTester.call(ActorIdTester::getCurrentActorId).get();
    Assert.assertEquals(actorId, actorIdTester.getId());
  }

  /**
   * Call this method each time to avoid hitting the cache in {@link RayObject#get()}.
   */
  static Runnable[] generateRunnables() {
    final RayObject<Integer> fooObject = Ray.put(1);
    final RayActor<Echo> fooActor = Ray.createActor(Echo::new);
    return new Runnable[]{
        () -> Ray.put(1),
        () -> Ray.get(fooObject.getId(), fooObject.getType()),
        fooObject::get,
        () -> Ray.wait(ImmutableList.of(fooObject)),
        Ray::getRuntimeContext,
        () -> Ray.call(MultiThreadingTest::echo, 1),
        () -> Ray.createActor(Echo::new),
        () -> fooActor.call(Echo::echo, 1),
    };
  }

  static boolean testMissingWrapRunnable() throws InterruptedException {
    {
      Runnable[] runnables = generateRunnables();
      // It's OK to run them in main thread.
      for (Runnable runnable : runnables) {
        runnable.run();
      }
    }

    Throwable[] throwable = new Throwable[1];

    {
      Runnable[] runnables = generateRunnables();
      Thread thread = new Thread(Ray.wrapRunnable(() -> {
        try {
          // It would be OK to run them in another thread if wrapped the runnable.
          for (Runnable runnable : runnables) {
            runnable.run();
          }
        } catch (Throwable ex) {
          throwable[0] = ex;
        }
      }));
      thread.start();
      thread.join();
      if (throwable[0] != null) {
        throw new RuntimeException("Exception occurred in thread.", throwable[0]);
      }
    }

    {
      Runnable[] runnables = generateRunnables();
      Thread thread = new Thread(() -> {
        try {
          // It wouldn't be OK to run them in another thread if not wrapped the runnable.
          for (Runnable runnable : runnables) {
            Assert.expectThrows(RayException.class, runnable::run);
          }
        } catch (Throwable ex) {
          throwable[0] = ex;
        }
      });
      thread.start();
      thread.join();
      if (throwable[0] != null) {
        throw new RuntimeException("Exception occurred in thread.", throwable[0]);
      }
    }

    {
      Runnable[] runnables = generateRunnables();
      Runnable[] wrappedRunnables = new Runnable[runnables.length];
      for (int i = 0; i < runnables.length; i++) {
        wrappedRunnables[i] = Ray.wrapRunnable(runnables[i]);
      }
      // It would be OK to run the wrapped runnables in the current thread.
      for (Runnable runnable : wrappedRunnables) {
        runnable.run();
      }

      // It would be OK to invoke Ray APIs after executing a wrapped runnable in the current thread.
      wrappedRunnables[0].run();
      runnables[0].run();
    }

    // Return true here to make the Ray.call returns an RayObject.
    return true;
  }

  @Test
  public void testMissingWrapRunnableInDriver() throws InterruptedException {
    testMissingWrapRunnable();
  }

  @Test
  public void testMissingWrapRunnableInWorker() {
    Ray.call(MultiThreadingTest::testMissingWrapRunnable).get();
  }

  @Test
  public void testGetAndSetAsyncContext() throws InterruptedException {
    Object asyncContext = Ray.getAsyncContext();
    Throwable[] throwable = new Throwable[1];
    Thread thread = new Thread(() -> {
      try {
        Ray.setAsyncContext(asyncContext);
        Ray.put(1);
      } catch (Throwable ex) {
        throwable[0] = ex;
      }
    });
    thread.start();
    thread.join();
    if (throwable[0] != null) {
      throw new RuntimeException("Exception occurred in thread.", throwable[0]);
    }
  }

  private static void runTestCaseInMultipleThreads(Runnable testCase, int numRepeats) {
    ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);

    try {
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < NUM_THREADS; i++) {
        Callable<String> task = Ray.wrapCallable(() -> {
          for (int j = 0; j < numRepeats; j++) {
            TimeUnit.MILLISECONDS.sleep(1);
            testCase.run();
          }
          return "ok";
        });
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

  private static boolean testGetAsyncContextAndSetAsyncContext() throws Exception {
    final Object asyncContext = Ray.getAsyncContext();
    final Object[] result = new Object[1];
    Thread thread = new Thread(() -> {
      try {
        Ray.setAsyncContext(asyncContext);
        Ray.put(0);
      } catch (Exception e) {
        result[0] = e;
      }
    });
    thread.start();
    thread.join();
    if (result[0] instanceof Exception) {
      throw (Exception) result[0];
    }
    return true;
  }

  public void testGetAsyncContextAndSetAsyncContextInDriver() throws Exception {
    Assert.assertTrue(testGetAsyncContextAndSetAsyncContext());
  }

  public void testGetAsyncContextAndSetAsyncContextInWorker() {
    RayObject<Boolean> obj = Ray.call(MultiThreadingTest::testGetAsyncContextAndSetAsyncContext);
    Assert.assertTrue(obj.get());
  }

}
