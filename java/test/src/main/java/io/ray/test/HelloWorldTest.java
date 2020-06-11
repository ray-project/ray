package io.ray.test;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
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
    ObjectRef<String> hello = Ray.call(HelloWorldTest::hello);
    ObjectRef<String> world = Ray.call(HelloWorldTest::world);
    String helloWorld = Ray.call(HelloWorldTest::merge, hello, world).get();
    Assert.assertEquals("hello,world!", helloWorld);
  }

}
