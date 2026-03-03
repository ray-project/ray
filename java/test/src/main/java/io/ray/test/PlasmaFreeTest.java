package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.object.ObjectRefImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PlasmaFreeTest extends BaseTest {

  private static String hello() {
    return "hello";
  }

  @Test
  public void testDeleteObjects() {
    ObjectRef<String> helloId = Ray.task(PlasmaFreeTest::hello).remote();
    String helloString = helloId.get();
    Assert.assertEquals("hello", helloString);
    Ray.internal().free(ImmutableList.of(helloId), true);

    final boolean result =
        TestUtils.waitForCondition(
            () ->
                !TestUtils.getRuntime()
                    .getObjectStore()
                    .wait(ImmutableList.of(((ObjectRefImpl<String>) helloId).getId()), 1, 0, true)
                    .get(0),
            50);
    if (TestUtils.isLocalMode()) {
      Assert.assertTrue(result);
    } else {
      // The object will not be deleted under cluster mode.
      Assert.assertFalse(result);
    }
  }
}
