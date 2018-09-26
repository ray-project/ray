package org.ray.runtime;

import com.google.common.base.Strings;
import java.lang.reflect.Field;
import java.util.stream.Collectors;
import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtime.RayRuntimeFactory;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.util.logger.RayLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default Ray runtime factory. It produces an instance of AbstractRayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRayRuntimeFactory.class);

  @Override
  public RayRuntime createRayRuntime() {
    RayLog.init();
    RayConfig rayConfig = RayConfig.create();
    try {
      AbstractRayRuntime runtime;
      if (rayConfig.runMode == RunMode.SINGLE_PROCESS) {
        runtime = new RayDevRuntime(rayConfig);
      } else {
        runtime = new RayNativeRuntime(rayConfig);
      }

      runtime.start();
      return runtime;
    } catch (Exception e) {
      LOGGER.error("Failed to initialize ray runtime", e);
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
