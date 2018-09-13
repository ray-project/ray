package org.ray.exercise;

import java.io.Serializable;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;

/**
 * Define a remote function, and execute multiple remote functions in parallel.
 */
public class Exercise01 implements Serializable {

  /**
   * A plain remote function.
   */
  // `@RayRemote` annotation converts a normal function to a remote function.
  @RayRemote
  public static String sayHello() {
    String ret = "hello";
    System.out.println(ret);
    return ret;
  }

  @RayRemote
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
      RayObject<String> hello = Ray.call(Exercise01::sayHello);
      RayObject<String> world = Ray.call(Exercise01::sayWorld);
      System.out.println("First remote call result:" + hello.get());
      System.out.println("Second remote call result:" + world.get());
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      Ray.shutdown();
    }
  }
}
