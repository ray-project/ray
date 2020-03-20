package org.ray.runtime;

import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtime.RayRuntimeFactory;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default Ray runtime factory. It produces an instance of RayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRayRuntimeFactory.class);

  @Override
  public RayRuntime createRayRuntime() {
    RayConfig rayConfig = RayConfig.getInstance();
    try {
      AbstractRayRuntime innerRuntime = rayConfig.runMode == RunMode.SINGLE_PROCESS
          ? new RayDevRuntime(rayConfig)
          : new RayNativeRuntime(rayConfig);
      RayRuntimeInternal runtime = rayConfig.numWorkersPerProcess > 1
          ? RayRuntimeProxy.newInstance(innerRuntime)
          : innerRuntime;
      runtime.start();
      return runtime;
    } catch (Exception e) {
      LOGGER.error("Failed to initialize ray runtime", e);
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
