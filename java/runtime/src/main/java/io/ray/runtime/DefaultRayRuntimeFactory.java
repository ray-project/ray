package io.ray.runtime;

import io.ray.api.runtime.RayRuntime;
import io.ray.api.runtime.RayRuntimeFactory;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.config.RunMode;
import io.ray.runtime.generated.Common.WorkerType;
import io.ray.runtime.util.LoggingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The default Ray runtime factory. It produces an instance of RayRuntime. */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  @Override
  public RayRuntime createRayRuntime() {
    RayConfig rayConfig = RayConfig.create();
    LoggingUtil.setupLogging(rayConfig);
    Logger logger = LoggerFactory.getLogger(DefaultRayRuntimeFactory.class);

    if (rayConfig.workerMode == WorkerType.WORKER) {
      // Handle the uncaught exceptions thrown from user-spawned threads.
      Thread.setDefaultUncaughtExceptionHandler(
          (Thread t, Throwable e) -> {
            logger.error(String.format("Uncaught worker exception in thread %s", t), e);
          });
    }

    try {
      logger.debug("Initializing runtime with config: {}", rayConfig);
      AbstractRayRuntime runtime =
          rayConfig.runMode == RunMode.LOCAL
              ? new RayDevRuntime(rayConfig)
              : new RayNativeRuntime(rayConfig);
      runtime.start();
      return runtime;
    } catch (Exception e) {
      logger.error("Failed to initialize ray runtime, with config " + rayConfig, e);
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
