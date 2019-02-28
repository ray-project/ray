package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RedisPasswordTest extends BaseTest {

  @Override
  public void beforeInitRay() {
    System.setProperty("ray.redis.head-password", "12345678");
    System.setProperty("ray.redis.password", "12345678");
  }

  @Override
  public void afterShutdownRay() {
    System.clearProperty("ray.redis.head-password");
    System.clearProperty("ray.redis.password");
  }

  @RayRemote
  public static String echo(String str) {
    return str;
  }

  @Test
  public void testRedisPassword() {
    RayObject<String> obj = Ray.call(RedisPasswordTest::echo, "hello");
    Assert.assertEquals("hello", obj.get());
  }

}
