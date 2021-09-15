package io.ray.serve.util;

import io.ray.api.Ray;

public class MetricsUtil {

  public static void registerMetrics(Runnable runnable) {
    if (!Ray.isInitialized() || Ray.getRuntimeContext().isSingleProcess()) {
      return;
    }
    runnable.run();
  }

  public static void xixihaha(Runnable runnable) {
    runnable.run();
  }
}
