package org.ray.api;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.ray.api.internal.RayConnector;
import org.ray.util.exception.TaskExecutionException;
import org.ray.util.logger.RayLog;

/**
 * Ray API.
 */
public final class Ray extends Rpc {

  private static RayApi impl = null;

  /**
   * initialize the current worker or the single-box cluster.
   */
  public static void init() {
    if (impl == null) {
      impl = RayConnector.run();
    }
  }

  /**
   * Put obj into object store.
   */
  public static <T> RayObject<T> put(T obj) {
    return impl.put(obj);
  }

  public static <T, TMT> RayObject<T> put(T obj, TMT metadata) {
    return impl.put(obj, metadata);
  }

  /**
   * Get obj(s) from object store.
   */
  static <T> T get(UniqueID objectId) throws TaskExecutionException {
    return impl.get(objectId);
  }

  static <T> List<T> get(List<UniqueID> objectIds) throws TaskExecutionException {
    return impl.get(objectIds);
  }

  static <T> T getMeta(UniqueID objectId) throws TaskExecutionException {
    return impl.getMeta(objectId);
  }

  static <T> List<T> getMeta(List<UniqueID> objectIds) throws TaskExecutionException {
    return impl.getMeta(objectIds);
  }

  /**
   * wait until timeout or enough RayObject are ready.
   *
   * @param waitfor             wait for who
   * @param numReturns          how many of ready is enough
   * @param timeoutMilliseconds in millisecond
   */
  public static <T> WaitResult<T> wait(List<RayObject<T>> waitfor, int numReturns,
                                       int timeoutMilliseconds) {
    return impl.wait(waitfor, numReturns, timeoutMilliseconds);
  }

  public static <T> WaitResult<T> wait(List<RayObject<T>> waitfor, int numReturns) {
    return impl.wait(waitfor, numReturns, Integer.MAX_VALUE);
  }

  public static <T> WaitResult<T> wait(List<RayObject<T>> waitfor) {
    return impl.wait(waitfor, waitfor.size(), Integer.MAX_VALUE);
  }

  /**
   * create actor object.
   */
  public static <T> RayActor<T> create(Class<T> cls) {
    try {
      if (cls.getConstructor() == null) {
        System.err.println("class " + cls.getName()
            + " does not (actors must) have a constructor with no arguments");
        RayLog.core.error("class {} does not (actors must) have a constructor with no arguments",
            cls.getName());
      }
    } catch (Exception e) {
      System.exit(1);
      return null;
    }
    return impl.create(cls);
  }

  /**
   * get underlying runtime.
   */
  static RayApi internal() {
    return impl;
  }
}
