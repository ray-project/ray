package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;

public class RedisPasswordTest extends BaseTest {
  @Override
  public void beforeInitRay() {
    System.setProperty("ray.redis.head-password", "12345678");
    System.setProperty("ray.redis.password", "12345678");
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
