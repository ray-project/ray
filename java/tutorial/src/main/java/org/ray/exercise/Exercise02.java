package org.ray.exercise;

import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;

/**
 * Execute remote functions in parallel with some dependencies.
 */
public class Exercise02 {

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

  /**
   * A remote function with dependency.
   */
  @RayRemote
  public static String merge(String hello, String world) {
    return hello + "," + world;
  }

  public static String sayHelloWorld() {
    RayObject<String> hello = Ray.call(Exercise02::sayHello);
    RayObject<String> world = Ray.call(Exercise02::sayWorld);
    // Pass unfinished results as the parameters to another remote function.
    return Ray.call(Exercise02::merge, hello, world).get();
  }

  public static void main(String[] args) throws Exception {
    try {
      Ray.init();
      String helloWorld = Exercise02.sayHelloWorld();
      System.out.println(helloWorld);
      assert helloWorld.equals("hello,world!");
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      Ray.shutdown();
    }
  }
}
