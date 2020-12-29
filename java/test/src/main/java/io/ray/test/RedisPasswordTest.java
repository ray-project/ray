package io.ray.test;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RedisPasswordTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.redis.password", "12345678");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.redis.password");
  }

  public static String echo(String str) {
    return str;
  }

  @Test
  public void testRedisPassword() {
    ObjectRef<String> obj = Ray.task(RedisPasswordTest::echo, "hello").remote();
    Assert.assertEquals("hello", obj.get());
  }
}
