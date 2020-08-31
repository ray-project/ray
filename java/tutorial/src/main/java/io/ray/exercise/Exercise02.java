package io.ray.exercise;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;

/**
 * Execute remote functions in parallel with some dependencies.
 */
public class Exercise02 {

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

  /**
   * A remote function with dependency.
   */
  public static String merge(String hello, String world) {
    return hello + "," + world;
  }

  public static String sayHelloWorld() {
    ObjectRef<String> hello = Ray.task(Exercise02::sayHello).remote();
    ObjectRef<String> world = Ray.task(Exercise02::sayWorld).remote();
    // Pass unfinished results as the parameters to another remote function.
    return Ray.task(Exercise02::merge, hello, world).remote().get();
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
