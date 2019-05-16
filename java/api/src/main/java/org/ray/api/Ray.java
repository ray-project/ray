package org.ray.api;

import java.util.List;
import org.ray.api.id.UniqueId;
import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtime.RayRuntimeFactory;
import org.ray.api.runtimecontext.RuntimeContext;

/**
 * This class contains all public APIs of Ray.
 */
public final class Ray extends RayCall {

  private static RayRuntime runtime = null;

  /**
   * Initialize Ray runtime with the default runtime implementation.
   */
  public static void init() {
    try {
      Class clz = Class.forName("org.ray.runtime.DefaultRayRuntimeFactory");
      RayRuntimeFactory factory = (RayRuntimeFactory) clz.newInstance();
      init(factory);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize Ray runtime.", e);
    }

  }

  /**
   * Initialize Ray runtime with a custom runtime implementation.
   *
   * @param factory A factory that produces the runtime instance.
   */
  public static synchronized void init(RayRuntimeFactory factory) {
    if (runtime == null) {
      runtime = factory.createRayRuntime();
      Runtime.getRuntime().addShutdownHook(new Thread(Ray::shutdown));
    }
  }

  /**
   * Shutdown Ray runtime.
   */
  public static void shutdown() {
    if (runtime != null) {
      runtime.shutdown();
      runtime = null;
    }
  }

  /**
   * Store an object in the object store.
   *
   * @param obj The Java object to be stored.
   * @return A RayObject instance that represents the in-store object.
   */
  public static <T> RayObject<T> put(T obj) {
    return runtime.put(obj);
  }

  /**
   * Get an object from the object store.
   *
   * @param objectId The ID of the object to get.
   * @return The Java object.
   */
  public static <T> T get(UniqueId objectId) {
    return runtime.get(objectId);
  }

  /**
   * Get a list of objects from the object store.
   *
   * @param objectIds The list of object IDs.
   * @return A list of Java objects.
   */
  public static <T> List<T> get(List<UniqueId> objectIds) {
    return runtime.get(objectIds);
  }

  /**
   * Wait for a list of RayObjects to be locally available,
   * until specified number of objects are ready, or specified timeout has passed.
   *
   * @param waitList A list of RayObject to wait for.
   * @param numReturns The number of objects that should be returned.
   * @param timeoutMs The maximum time in milliseconds to wait before returning.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public static <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns,
      int timeoutMs) {
    return runtime.wait(waitList, numReturns, timeoutMs);
  }

  /**
   * A convenient helper method for Ray.wait. It will wait infinitely until
   * specified number of objects are locally available.
   *
   * @param waitList A list of RayObject to wait for.
   * @param numReturns The number of objects that should be returned.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public static <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns) {
    return runtime.wait(waitList, numReturns, Integer.MAX_VALUE);
  }

  /**
   * A convenient helper method for Ray.wait. It will wait infinitely until
   * all objects are locally available.
   *
   * @param waitList A list of RayObject to wait for.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public static <T> WaitResult<T> wait(List<RayObject<T>> waitList) {
    return runtime.wait(waitList, waitList.size(), Integer.MAX_VALUE);
  }

  /**
   * Get the underlying runtime instance.
   */
  public static RayRuntime internal() {
    return runtime;
  }

  /**
   * Get the runtime context.
   */
  public static RuntimeContext getRuntimeContext() {
    return runtime.getRuntimeContext();
  }
}
