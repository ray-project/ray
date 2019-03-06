package org.ray.api;

import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.config.RunMode;
import org.testng.SkipException;

public class TestUtils {

  public static void skipTestUnderSingleProcess() {
    AbstractRayRuntime runtime = (AbstractRayRuntime)Ray.internal();
    if (runtime.getRayConfig().runMode == RunMode.SINGLE_PROCESS) {
      throw new SkipException("Skip case.");
    }
  }
}
