package io.ray.docdemo;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import org.testng.Assert;

/**
 * This class contains demo code of the Ray introduction doc
 * (https://docs.ray.io/en/master/index.html and
 * https://docs.ray.io/en/master/ray-overview/index.html).
 *
 * <p>Please keep them in sync.
 */
public class SimpleJob {

  public static String envInNormalTask() {
    return System.getenv("TEST_KEY");
  }

  public static int square(int x) {
    return x * x;
  }

  public static class Counter {

    private int value = 0;

    public boolean increment() {
      this.value += 1;
      return true;
    }

    public int read() {
      return this.value;
    }
  }

  public static void main(String[] args) {
    // Intialize Ray runtime.
    Ray.init();
    String testValue = System.getenv("TEST_KEY");
    System.out.println("try to get TEST_KEY: " + testValue);

    String testValueFromNormalTask = Ray.get(Ray.task(SimpleJob::envInNormalTask).remote());
    System.out.println("try to get TEST_KEY from normal task: " + testValueFromNormalTask);

    for (int i = 0; i < 4; i++) {
      int res = Ray.get(Ray.task(SimpleJob::square, i).remote());
      Assert.assertEquals(res, i * i);
    }

    ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(Ray.get(counter.task(Counter::increment).remote()));
    }
    int res = Ray.get(counter.task(Counter::read).remote());
    Assert.assertEquals(res, 4);
  }
}
