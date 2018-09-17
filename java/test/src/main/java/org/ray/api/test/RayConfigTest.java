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

    Assert.assertEquals(rayConfig.workerMode, WorkerMode.DRIVER);
    Assert.assertEquals(rayConfig.runMode, RunMode.SINGLE_BOX);
  }
}
