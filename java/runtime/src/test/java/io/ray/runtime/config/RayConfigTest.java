package io.ray.runtime.config;

import io.ray.runtime.generated.Common.WorkerType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayConfigTest {

  public static final int NUM_RETRIES = 5;

  @Test
  public void testCreateRayConfig() {
    try {
      System.setProperty("ray.job.resource-path", "path/to/ray/job/resource/path");
      RayConfig.reset();
      RayConfig rayConfig = RayConfig.getInstance();
      Assert.assertEquals(WorkerType.DRIVER, rayConfig.workerMode);
      Assert.assertEquals("path/to/ray/job/resource/path", rayConfig.jobResourcePath);
    } finally {
      // Unset system properties.
      System.clearProperty("ray.job.resource-path");
    }
  }

  @Test
  public void testGenerateHeadPortRandomly() {
    boolean isSame = true;
    RayConfig.reset();
    final int port1 = RayConfig.getInstance().headRedisPort;
    // If we the 2 ports are the same, let's retry.
    // This is used to avoid any flaky chance.
    for (int i = 0; i < NUM_RETRIES; ++i) {
      RayConfig.reset();
      final int port2 = RayConfig.getInstance().headRedisPort;
      if (port1 != port2) {
        isSame = false;
        break;
      }
    }
    Assert.assertFalse(isSame);
  }

  @Test
  public void testSpecifyHeadPort() {
    System.setProperty("ray.redis.head-port", "11111");
    try {
      RayConfig.reset();
      Assert.assertEquals(RayConfig.getInstance().headRedisPort, 11111);
    } finally {
      System.clearProperty("ray.redis.head-port");
    }
  }
}
