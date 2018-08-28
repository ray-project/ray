package org.ray.api;

import java.util.List;
import org.ray.api.internal.RayConnector;

/**
 * Ray API.
 */
public final class Ray extends RayCall {

  private static RayRuntime impl = null;

  /**
   * initialize the current worker or the single-box cluster.
   */
  public static void init() {
    if (impl == null) {
      impl = RayConnector.run();
    }
  }

  public static <T> RayObject<T> put(T obj) {
    return impl.put(obj);
  }

  public static <T> T get(UniqueID objectId) {
    return impl.get(objectId);
  }

  public static <T> List<T> get(List<UniqueID> objectIds) {
    return impl.get(objectIds);
  }

  public static <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns,
                                       int timeoutMs) {
    return impl.wait(waitList, numReturns, timeoutMs);
  }

  public static <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns) {
    return impl.wait(waitList, numReturns, Integer.MAX_VALUE);
  }

  public static <T> WaitResult<T> wait(List<RayObject<T>> waitList) {
    return impl.wait(waitList, waitList.size(), Integer.MAX_VALUE);
  }

  public static <T> RayActor<T> createActor(Class<T> actorClass) {
    return impl.createActor(actorClass);
  }

  /**
   * get underlying runtime.
   */
  static RayRuntime internal() {
    return impl;
  }
}
