package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import org.ray.api.Ray;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.RayMultiWorkerNativeRuntime;

public class RuntimeUtil {

  public static AbstractRayRuntime getRuntime() {
    RayRuntime runtime = Ray.internal();
    if (runtime instanceof RayMultiWorkerNativeRuntime) {
      runtime = ((RayMultiWorkerNativeRuntime) runtime).getCurrentRuntime();
    }
    Preconditions.checkState(runtime instanceof AbstractRayRuntime);
    return (AbstractRayRuntime) runtime;
  }
}
