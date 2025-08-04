package io.ray.runtime.runner.worker;

import io.ray.api.Ray;
import io.ray.runtime.AbstractRayRuntime;

/** Default implementation of the worker process. */
public class DefaultWorker {

  public static void main(String[] args) {
    // Set run-mode to `CLUSTER` explicitly, to prevent the DefaultWorker to receive
    // a wrong run-mode parameter through jvm options.
    System.setProperty("ray.run-mode", "CLUSTER");
    System.setProperty("ray.worker.mode", "WORKER");
    Ray.init();
    ((AbstractRayRuntime) Ray.internal()).run();
  }
}
