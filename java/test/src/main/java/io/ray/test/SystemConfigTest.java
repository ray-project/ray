package io.ray.test;

import io.ray.runtime.util.SystemConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class SystemConfigTest extends BaseTest {

  public void testDefaultConfigs() {
    long ret = ((Double) SystemConfig.get("max_direct_call_object_size")).longValue();
    Assert.assertEquals(ret, 100 * 1024);
    boolean ret2 = (boolean) SystemConfig.get("worker_process_watchdog_enabled");
    Assert.assertTrue(ret2);
  }
}
