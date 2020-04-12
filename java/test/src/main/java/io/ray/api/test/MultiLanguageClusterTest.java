package io.ray.api.test;

import io.ray.api.Ray;
import io.ray.api.RayObject;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MultiLanguageClusterTest extends BaseMultiLanguageTest {

  public static String echo(String word) {
    return word;
  }

  @Test
  public void testMultiLanguageCluster() {
    RayObject<String> obj = Ray.call(MultiLanguageClusterTest::echo, "hello");
    Assert.assertEquals("hello", obj.get());
  }

}
