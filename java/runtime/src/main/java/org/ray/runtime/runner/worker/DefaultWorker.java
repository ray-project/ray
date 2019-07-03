package org.ray.runtime.runner.worker;

import org.ray.api.Ray;
import org.ray.runtime.AbstractRayRuntime;
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
      Thread.setDefaultUncaughtExceptionHandler((Thread t, Throwable e) -> {
        LOGGER.error("Uncaught worker exception in thread {}: {}", t, e);
      });
      Ray.init();
      LOGGER.info("Worker started.");
      ((AbstractRayRuntime)Ray.internal()).loop();
    } catch (Exception e) {
      LOGGER.error("Failed to start worker.", e);
    }
  }
}
