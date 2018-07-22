package org.ray.exercise;

import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.core.RayRuntime;
import org.ray.example.HelloWorld;

/**
 * Execute remote functions in parallel with some dependencies.
 */
public class Exercise02 {

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
   * Remote function with dependency.
   */
  @RayRemote
  public static String merge(String hello, String world) {
    return hello + "," + world;
  }

  /**
   * Entry function.
   */
  public static String sayHelloWorld() {
    RayObject<String> hello = Ray.call(HelloWorld::sayHello);
    RayObject<String> world = Ray.call(HelloWorld::sayWorld);
    return Ray.call(HelloWorld::merge, hello, world).get();
  }

  /**
   * Main.
   */
  public static void main(String[] args) throws Exception {
    try {
      Ray.init();
      String helloWorld = HelloWorld.sayHelloWorld();
      System.out.println(helloWorld);
      assert helloWorld.equals("hello,world!");
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      RayRuntime.getInstance().cleanUp();
    }
  }
}
