package io.ray.test;

import io.ray.runtime.util.SystemConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class SystemConfigTest extends BaseTest {

  public void testDefaultConfigs() {
    long ret = ((Double) SystemConfig.get("max_direct_call_object_size")).longValue();
    Assert.assertEquals(ret, 100 * 1024);
  }
}
