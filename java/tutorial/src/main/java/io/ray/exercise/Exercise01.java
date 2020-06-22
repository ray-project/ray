package io.ray.exercise;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.io.Serializable;

/**
 * Define a remote function, and execute multiple remote functions in parallel.
 */
public class Exercise01 implements Serializable {

  /**
   * A plain remote function.
   */
  public static String sayHello() {
    String ret = "hello";
    System.out.println(ret);
    return ret;
  }

  public static String sayWorld() {
    String ret = "world!";
    System.out.println(ret);
    return ret;
  }

  public static void main(String[] args) throws Exception {
    try {
      // Use `Ray.init` to initialize the Ray runtime.
      Ray.init();
      // Use `Ray.call` to call a remote function.
      ObjectRef<String> hello = Ray.task(Exercise01::sayHello).remote();
      ObjectRef<String> world = Ray.task(Exercise01::sayWorld).remote();
      System.out.println("First remote call result:" + hello.get());
      System.out.println("Second remote call result:" + world.get());
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      Ray.shutdown();
    }
  }
}
