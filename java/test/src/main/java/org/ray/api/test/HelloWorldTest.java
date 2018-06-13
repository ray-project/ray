package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.util.logger.RayLog;

/**
 * Hello world.
 */
@RunWith(MyRunner.class)
public class HelloWorldTest {

  @RayRemote
  public static String sayHello() {
    String ret = "he";
    ret += "llo";
    RayLog.rapp.info("real say hello");
    //throw new RuntimeException("+++++++++++++++++++++hello exception");
    return ret;
  }

  @RayRemote
  public static String sayWorld() {
    String ret = "world";
    ret += "!";
    return ret;
  }

  @RayRemote
  public static String merge(String hello, String world) {
    return hello + "," + world;
  }

  @Test
  public void test() {
    String helloWorld = sayHelloWorld();
    RayLog.rapp.info(helloWorld);
    Assert.assertEquals("hello,world!", helloWorld);
    Assert.assertTrue(Ray.call(TypesTest::sayBool).get());
  }

  public String sayHelloWorld() {
    RayObject<String> hello = Ray.call(HelloWorldTest::sayHello);
    RayObject<String> world = Ray.call(HelloWorldTest::sayWorld);
    return Ray.call(HelloWorldTest::merge, hello, world).get();
  }
}
