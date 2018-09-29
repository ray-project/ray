package org.ray.runtime.runner.worker;

import org.ray.api.Ray;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.util.logger.RayLog;

/**
 * Default implementation of the worker process.
 */
public class DefaultWorker {

  public static void main(String[] args) {
    RayLog.init();
    System.setProperty("ray.mode", "WORKER");
    // if any exception, just throw it
    Ray.init();

    ((AbstractRayRuntime) Ray.internal()).loop();
  }
}
