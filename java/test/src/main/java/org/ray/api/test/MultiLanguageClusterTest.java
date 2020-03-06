package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayObject;
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
