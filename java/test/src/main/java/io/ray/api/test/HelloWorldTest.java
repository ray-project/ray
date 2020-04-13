package io.ray.api.test;

import io.ray.api.Ray;
import io.ray.api.RayObject;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Hello world.
 */
public class HelloWorldTest extends BaseTest {

  private static String hello() {
    return "hello";
  }

  private static String world() {
    return "world!";
  }

  private static String merge(String hello, String world) {
    return hello + "," + world;
  }

  @Test
  public void testHelloWorld() {
    RayObject<String> hello = Ray.call(HelloWorldTest::hello);
    RayObject<String> world = Ray.call(HelloWorldTest::world);
    String helloWorld = Ray.call(HelloWorldTest::merge, hello, world).get();
    Assert.assertEquals("hello,world!", helloWorld);
  }

}
