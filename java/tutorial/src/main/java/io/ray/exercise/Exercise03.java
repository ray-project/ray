package io.ray.exercise;

import io.ray.api.Ray;
import io.ray.api.RayObject;

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
    RayObject<String> world = Ray.call(Exercise03::sayWorld);
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
      String helloWithWorld = Ray.call(Exercise03::sayHelloWithWorld).get();
      System.out.println(helloWithWorld);
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      Ray.shutdown();
    }
  }
}
