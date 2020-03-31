package io.ray.runtime;

import io.ray.api.runtime.RayRuntime;
import io.ray.api.runtime.RayRuntimeFactory;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.config.RunMode;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.generated.Common.WorkerType;
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
      FunctionManager functionManager = new FunctionManager(rayConfig.jobResourcePath);
      RayRuntime runtime;
      if (rayConfig.runMode == RunMode.SINGLE_PROCESS) {
        runtime = new RayDevRuntime(rayConfig, functionManager);
      } else {
        if (rayConfig.workerMode == WorkerType.DRIVER) {
          runtime = new RayNativeRuntime(rayConfig, functionManager);
        } else {
          runtime = new RayMultiWorkerNativeRuntime(rayConfig, functionManager);
        }
      }
      return runtime;
    } catch (Exception e) {
      LOGGER.error("Failed to initialize ray runtime", e);
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
