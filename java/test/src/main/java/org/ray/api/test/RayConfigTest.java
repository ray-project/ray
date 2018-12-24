package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.config.WorkerMode;

public class RayConfigTest {

  @Test
  public void testCreateRayConfig() {
    try {
      System.setProperty("ray.home", "/path/to/ray");
      System.setProperty("ray.driver.resource-path", "path/to/ray/driver/resource/path");
      RayConfig rayConfig = RayConfig.create();

      Assert.assertEquals("/path/to/ray", rayConfig.rayHome);
      Assert.assertEquals(WorkerMode.DRIVER, rayConfig.workerMode);
      Assert.assertEquals(RunMode.CLUSTER, rayConfig.runMode);

      System.setProperty("ray.home", "");
      rayConfig = RayConfig.create();

      Assert.assertEquals(System.getProperty("user.dir"), rayConfig.rayHome);
      Assert.assertEquals(System.getProperty("user.dir") +
          "/build/src/ray/thirdparty/redis/src/redis-server", rayConfig.redisServerExecutablePath);

      Assert.assertEquals("path/to/ray/driver/resource/path", rayConfig.driverResourcePath);
    } finally {
      //unset the system property
      System.clearProperty("ray.home");
      System.clearProperty("ray.driver.resource-path");
    }

  }
}
