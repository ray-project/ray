package io.ray.exercise;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;

/**
 * Call a remote function from within another remote function.
 */
public class Exercise03 {

  /**
   * A remote function which will call another remote function.
   */
  public static String sayHelloWithWorld() {
    String ret = "hello";
    System.out.println(ret);
    ObjectRef<String> world = Ray.task(Exercise03::sayWorld).remote();
    return ret + "," + world.get();
  }

  /**
   * A remote function which will be called by another remote function.
   */
  public static String sayWorld() {
    String ret = "world!";
    System.out.println(ret);
    return ret;
  }

  public static void main(String[] args) throws Exception {
    try {
      Ray.init();
      String helloWithWorld = Ray.task(Exercise03::sayHelloWithWorld).remote().get();
      System.out.println(helloWithWorld);
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      Ray.shutdown();
    }
  }
}
