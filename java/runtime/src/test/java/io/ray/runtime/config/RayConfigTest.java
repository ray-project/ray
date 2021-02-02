package io.ray.runtime.config;

import io.ray.runtime.generated.Common.WorkerType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayConfigTest {

  public static final int NUM_RETRIES = 5;

  @Test
  public void testCreateRayConfig() {
    try {
      System.setProperty("ray.job.code-search-path", "path/to/ray/job/resource/path");
      RayConfig rayConfig = RayConfig.create();
      Assert.assertEquals(WorkerType.DRIVER, rayConfig.workerMode);
      Assert.assertEquals(
          Collections.singletonList("path/to/ray/job/resource/path"), rayConfig.codeSearchPath);
    } finally {
      // Unset system properties.
      System.clearProperty("ray.job.code-search-path");
    }
  }
}
