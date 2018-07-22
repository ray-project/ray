package org.ray.exercise;

import java.io.Serializable;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.core.RayRuntime;

/**
 * Define a remote function, and execute multiple remote functions in parallel.
 */
public class Exercise01 implements Serializable {

  /**
   * Remote function.
   */
  @RayRemote
  public static String sayHello() {
    String ret = "hello";
    System.out.println(ret);
    return ret;
  }

  /**
   * Remote function.
   */
  @RayRemote
  public static String sayWorld() {
    String ret = "world!";
    System.out.println(ret);
    return ret;
  }

  /**
   * Main.
   */
  public static void main(String[] args) throws Exception {
    try {
      Ray.init();
      RayObject<String> hello = Ray.call(Exercise01::sayHello);
      RayObject<String> world = Ray.call(Exercise01::sayWorld);
      System.out.println("first remote call result:" + hello.get());
      System.out.println("second remote call result:" + world.get());
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      RayRuntime.getInstance().cleanUp();
    }
  }
}
