package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;

/**
 * Hello world.
 */
@RunWith(MyRunner.class)
public class HelloWorldTest {

  @RayRemote
  private static String hello() {
    return "hello";
  }

  @RayRemote
  private static String world() {
    return "world!";
  }

  @RayRemote
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
