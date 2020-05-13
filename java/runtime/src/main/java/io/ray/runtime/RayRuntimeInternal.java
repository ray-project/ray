package io.ray.runtime;

import io.ray.api.runtime.RayRuntime;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.context.WorkerContext;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.gcs.GcsClient;
import io.ray.runtime.object.ObjectStore;

/**
 * This interface is required to make {@link RayRuntimeProxy} work.
 */
public interface RayRuntimeInternal extends RayRuntime {

  /**
   * Start runtime.
   */
  void start();

  WorkerContext getWorkerContext();

  ObjectStore getObjectStore();

  FunctionManager getFunctionManager();

  RayConfig getRayConfig();

  GcsClient getGcsClient();

  void setIsContextSet(boolean isContextSet);

  void run();
}
