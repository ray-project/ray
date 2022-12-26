package io.ray.serve.replica;

import io.ray.serve.common.Constants;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CallableUtilsTest {

  /** Method: getProvider(String ingress) */
  @Test
  public void testGetProvider() throws Exception {
    ServeCallableProvider defaultProvider =
        CallableUtils.getProvider(Constants.CALLABLE_PROVIDER_DEFAULT);
    Assert.assertNotNull(defaultProvider);
    ServeCallableProvider jerseyProvider =
        CallableUtils.getProvider(Constants.CALLABLE_PROVIDER_JERSEY);
    Assert.assertNotNull(jerseyProvider);
    ServeCallableProvider tempProvider1 = CallableUtils.getProvider(null);
    Assert.assertEquals(tempProvider1, defaultProvider);
    try {
      CallableUtils.getProvider("not exist type");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(
          e.getMessage(), "can not find the ServeCallableProvider, type is not exist type");
    }
  }
}
