package io.ray.runtime.runner.worker;

import io.ray.api.Ray;
import io.ray.runtime.RayRuntimeInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the worker process.
 */
public class DefaultWorker {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultWorker.class);

  public static void main(String[] args) {
    try {
      System.setProperty("ray.worker.mode", "WORKER");
      // Set run-mode to `CLUSTER` explicitly, to prevent the DefaultWorker to receive
      // a wrong run-mode parameter through jvm options.
      System.setProperty("ray.run-mode", "CLUSTER");
      Thread.setDefaultUncaughtExceptionHandler((Thread t, Throwable e) -> {
        LOGGER.error("Uncaught worker exception in thread {}: {}", t, e);
      });
      Ray.init();
      LOGGER.info("Worker started.");
      ((RayRuntimeInternal) Ray.internal()).run();
    } catch (Exception e) {
      LOGGER.error("Failed to start worker.", e);
    }
  }
}
