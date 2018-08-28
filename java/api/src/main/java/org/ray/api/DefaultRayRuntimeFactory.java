package org.ray.api;

import java.lang.reflect.Method;

/**
 * The default Ray runtime factory. It produces an instance of BaseRayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  @Override
  public RayRuntime createRayRuntime() {
    try {
      Method m = Class.forName("org.ray.core.BaseRayRuntime").getDeclaredMethod("init");
      m.setAccessible(true);
      RayRuntime runtime = (RayRuntime) m.invoke(null);
      m.setAccessible(false);
      return runtime;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
