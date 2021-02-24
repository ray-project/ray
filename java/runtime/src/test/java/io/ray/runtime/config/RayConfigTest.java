package io.ray.runtime.config;

import io.ray.runtime.generated.Common.WorkerType;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayConfigTest {

  @Test
  public void testCreateRayConfig() {
    System.setProperty("ray.job.code-search-path", "path/to/ray/job/resource/path");
    RayConfig rayConfig = RayConfig.create();
    Assert.assertEquals(WorkerType.DRIVER, rayConfig.workerMode);
    Assert.assertEquals(
        Collections.singletonList("path/to/ray/job/resource/path"), rayConfig.codeSearchPath);
  }
}
