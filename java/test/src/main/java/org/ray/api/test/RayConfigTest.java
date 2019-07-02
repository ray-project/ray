package org.ray.api.test;

import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.WorkerMode;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayConfigTest {

  @Test
  public void testCreateRayConfig() {
    try {
      System.setProperty("ray.job.resource-path", "path/to/ray/job/resource/path");
      RayConfig rayConfig = RayConfig.create();
      Assert.assertEquals(WorkerMode.DRIVER, rayConfig.workerMode);
      Assert.assertEquals("path/to/ray/job/resource/path", rayConfig.jobResourcePath);
    } finally {
      // Unset system properties.
      System.clearProperty("ray.job.resource-path");
    }

  }
}
