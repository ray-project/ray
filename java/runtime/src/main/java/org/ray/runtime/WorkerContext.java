package org.ray.runtime;

import org.ray.runtime.util.RayObjectValueConverter;

public class WorkerContext {
  private ClassLoader currentClassLoader;

  private RayObjectValueConverter rayObjectValueConverter = new RayObjectValueConverter(null);

  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    if (this.currentClassLoader != currentClassLoader) {
      this.currentClassLoader = currentClassLoader;
      rayObjectValueConverter = new RayObjectValueConverter(currentClassLoader);
    }
  }

  public RayObjectValueConverter getRayObjectValueConverter() {
    return rayObjectValueConverter;
  }
}
