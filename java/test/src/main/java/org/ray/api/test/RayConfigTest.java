package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.config.WorkerMode;

public class RayConfigTest {

  @Test
  public void testCreateRayConfig() {
    System.setProperty("ray.home", "/path/to/ray");
    RayConfig rayConfig = RayConfig.create();

    Assert.assertEquals("/path/to/ray", rayConfig.rayHome);
    Assert.assertEquals(WorkerMode.DRIVER, rayConfig.workerMode);
    Assert.assertEquals(RunMode.CLUSTER, rayConfig.runMode);

    System.setProperty("ray.home", "");
    rayConfig = RayConfig.create();
    Assert.assertEquals(System.getProperty("user.dir"), rayConfig.rayHome);
    Assert.assertEquals(System.getProperty("user.dir") +
        "/build/src/common/thirdparty/redis/src/redis-server", rayConfig.redisServerExecutablePath);
  }
}
