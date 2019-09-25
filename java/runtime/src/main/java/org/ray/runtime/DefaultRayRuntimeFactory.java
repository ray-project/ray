package org.ray.runtime;

import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtime.RayRuntimeFactory;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.generated.Common.WorkerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default Ray runtime factory. It produces an instance of RayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRayRuntimeFactory.class);

  @Override
  public RayRuntime createRayRuntime() {
    RayConfig rayConfig = RayConfig.create();
    try {
      RayRuntime runtime;
      if (rayConfig.runMode == RunMode.SINGLE_PROCESS) {
        runtime = new RayDevRuntime(rayConfig);
      } else {
        if (rayConfig.workerMode == WorkerType.DRIVER) {
          runtime = new RayNativeRuntime(rayConfig);
        } else {
          runtime = new RayMultiWorkerNativeRuntime(rayConfig);
        }
      }
      return runtime;
    } catch (Exception e) {
      LOGGER.error("Failed to initialize ray runtime", e);
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
