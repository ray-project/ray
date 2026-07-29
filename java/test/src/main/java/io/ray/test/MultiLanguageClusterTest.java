package io.ray.test;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class MultiLanguageClusterTest extends BaseTest {

  public static String echo(String word) {
    return word;
  }

  @Test
  public void testMultiLanguageCluster() {
    ObjectRef<String> obj = Ray.task(MultiLanguageClusterTest::echo, "hello").remote();
    Assert.assertEquals("hello", obj.get());
  }
}
