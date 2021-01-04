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
    Map<String, String> rayletConfig = new HashMap<>();
    rayletConfig.put("one", "1");
    rayletConfig.put("zero", "0");
    rayletConfig.put("positive-integer", "123");
    rayletConfig.put("negative-integer", "-123");
    rayletConfig.put("float", "-123.456");
    rayletConfig.put("true", "true");
    rayletConfig.put("false", "false");
    rayletConfig.put("string", "abc");

    try {
      System.setProperty("ray.job.code-search-path", "path/to/ray/job/resource/path");
      for (Map.Entry<String, String> entry : rayletConfig.entrySet()) {
        System.setProperty("ray.raylet.config." + entry.getKey(), entry.getValue());
      }
      RayConfig rayConfig = RayConfig.create();
      Assert.assertEquals(WorkerType.DRIVER, rayConfig.workerMode);
      Assert.assertEquals(
          Collections.singletonList("path/to/ray/job/resource/path"), rayConfig.codeSearchPath);
      Assert.assertEquals(rayConfig.rayletConfigParameters.get("one"), 1);
      Assert.assertEquals(rayConfig.rayletConfigParameters.get("zero"), 0);
      Assert.assertEquals(rayConfig.rayletConfigParameters.get("positive-integer"), 123);
      Assert.assertEquals(rayConfig.rayletConfigParameters.get("negative-integer"), -123);
      Assert.assertEquals(rayConfig.rayletConfigParameters.get("float"), -123.456f);
      Assert.assertEquals(rayConfig.rayletConfigParameters.get("true"), true);
      Assert.assertEquals(rayConfig.rayletConfigParameters.get("false"), false);
      Assert.assertEquals(rayConfig.rayletConfigParameters.get("string"), "abc");
    } finally {
      // Unset system properties.
      System.clearProperty("ray.job.code-search-path");
      for (String key : rayletConfig.keySet()) {
        System.clearProperty("ray.raylet.config." + key);
      }
    }
  }
}
