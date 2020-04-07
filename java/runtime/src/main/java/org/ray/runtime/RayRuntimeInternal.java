package org.ray.runtime;

import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.context.WorkerContext;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.object.ObjectStore;

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
