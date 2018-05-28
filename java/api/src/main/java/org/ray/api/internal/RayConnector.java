package org.ray.api.internal;

import java.lang.reflect.Method;
import org.ray.api.RayApi;
import org.ray.util.logger.RayLog;

/**
 * Mediator, which pulls the {@code org.ray.api.RayApi} up to run.
 */
public class RayConnector {

  private static final String className = "org.ray.core.RayRuntime";

  public static RayApi run() {
    try {
      Method m = Class.forName(className).getDeclaredMethod("init");
      m.setAccessible(true);
      RayApi api = (RayApi) m.invoke(null);
      m.setAccessible(false);
      return api;
    } catch (ReflectiveOperationException | IllegalArgumentException | SecurityException e) {
      RayLog.core.error("Load " + className + " class failed.", e);
      throw new Error("RayApi is not successfully initiated.");
    }
  }
}
