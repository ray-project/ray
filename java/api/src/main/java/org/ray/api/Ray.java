package org.ray.api;

import java.util.List;
import org.ray.api.internal.RayConnector;

/**
 * Ray API.
 */
public final class Ray extends RayCall {

  private static RayRuntime runtime = null;

  /**
   * Initialize Ray runtime.
   */
  public static void init() {
    init(new DefaultRayRuntimeFactory());
  }

  synchronized public static void init(RayRuntimeFactory factory) {
    if (runtime != null) {
      throw new RuntimeException("Ray runtime was already initialized.");
    }
    runtime = factory.createRayRuntime();
  }

  public static void shutdown() {
    runtime.shutdown();
  }

  public static <T> RayObject<T> put(T obj) {
    return runtime.put(obj);
  }

  public static <T> T get(UniqueID objectId) {
    return runtime.get(objectId);
  }

  public static <T> List<T> get(List<UniqueID> objectIds) {
    return runtime.get(objectIds);
  }

  public static <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns,
                                       int timeoutMs) {
    return runtime.wait(waitList, numReturns, timeoutMs);
  }

  public static <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns) {
    return runtime.wait(waitList, numReturns, Integer.MAX_VALUE);
  }

  public static <T> WaitResult<T> wait(List<RayObject<T>> waitList) {
    return runtime.wait(waitList, waitList.size(), Integer.MAX_VALUE);
  }

  public static <T> RayActor<T> createActor(Class<T> actorClass) {
    return runtime.createActor(actorClass);
  }

  /**
   * get underlying runtime.
   */
  static RayRuntime internal() {
    return runtime;
  }
}
