package org.ray.api.internal;

import java.lang.reflect.Method;
import org.ray.api.RayRuntime;

/**
 * Mediator, which pulls the {@code org.ray.api.RayRuntime} up to run.
 */
public class RayConnector {

  private static final String className = "org.ray.core.RayRuntime";

  public static RayRuntime run() {
    try {
      Method m = Class.forName(className).getDeclaredMethod("init");
      m.setAccessible(true);
      RayRuntime api = (RayRuntime) m.invoke(null);
      m.setAccessible(false);
      return api;
    } catch (ReflectiveOperationException | IllegalArgumentException | SecurityException e) {
//      RayLog.core.error("Load {} class failed.", className, e);
      throw new Error("RayRuntime is not successfully initiated.");
    }
  }
}
