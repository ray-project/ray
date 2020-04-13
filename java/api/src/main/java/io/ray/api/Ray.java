package io.ray.api;

import io.ray.api.id.ObjectId;
import io.ray.api.id.UniqueId;
import io.ray.api.runtime.RayRuntime;
import io.ray.api.runtime.RayRuntimeFactory;
import io.ray.api.runtimecontext.RuntimeContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

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
      Class clz = Class.forName("io.ray.runtime.DefaultRayRuntimeFactory");
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
  public static synchronized void shutdown() {
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
   * Get an object by id from the object store.
   *
   * @param objectId The ID of the object to get.
   * @param objectType The type of the object to get.
   * @return The Java object.
   */
  public static <T> T get(ObjectId objectId, Class<T> objectType) {
    return runtime.get(objectId, objectType);
  }

  /**
   * Get a list of objects by ids from the object store.
   *
   * @param objectIds The list of object IDs.
   * @param objectType The type of object.
   * @return A list of Java objects.
   */
  public static <T> List<T> get(List<ObjectId> objectIds, Class<T> objectType) {
    return runtime.get(objectIds, objectType);
  }

  /**
   * Get a list of objects by RayObjects from the object store.
   *
   * @param objectList A list of RayObject to get.
   * @return A list of Java objects.
   */
  public static <T> List<T> get(List<RayObject<T>> objectList) {
    List<ObjectId> objectIds = new ArrayList<>();
    Class<T> objectType = null;
    for (RayObject<T> o : objectList) {
      objectIds.add(o.getId());
      objectType = o.getType();
    }
    return runtime.get(objectIds, objectType);
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
   * If users want to use Ray API in their own threads, call this method to get the async context
   * and then call {@link #setAsyncContext} at the beginning of the new thread.
   *
   * @return The async context.
   */
  public static Object getAsyncContext() {
    return runtime.getAsyncContext();
  }

  /**
   * Set the async context for the current thread.
   *
   * @param asyncContext The async context to set.
   */
  public static void setAsyncContext(Object asyncContext) {
    runtime.setAsyncContext(asyncContext);
  }

  // TODO (kfstorm): add the `rollbackAsyncContext` API to allow rollbacking the async context of
  // the current thread to the one before `setAsyncContext` is called.

  // TODO (kfstorm): unify the `wrap*` methods.

  /**
   * If users want to use Ray API in their own threads, they should wrap their {@link Runnable}
   * objects with this method.
   *
   * @param runnable The runnable to wrap.
   * @return The wrapped runnable.
   */
  public static Runnable wrapRunnable(Runnable runnable) {
    return runtime.wrapRunnable(runnable);
  }

  /**
   * If users want to use Ray API in their own threads, they should wrap their {@link Callable}
   * objects with this method.
   *
   * @param callable The callable to wrap.
   * @return The wrapped callable.
   */
  public static <T> Callable<T> wrapCallable(Callable<T> callable) {
    return runtime.wrapCallable(callable);
  }

  /**
   * Get the underlying runtime instance.
   */
  public static RayRuntime internal() {
    return runtime;
  }

  /**
   * Update the resource for the specified client.
   * Set the resource for the specific node.
   */
  public static void setResource(UniqueId nodeId, String resourceName, double capacity) {
    runtime.setResource(resourceName, capacity, nodeId);
  }

  /**
   * Set the resource for local node.
   */
  public static void setResource(String resourceName, double capacity) {
    runtime.setResource(resourceName, capacity, UniqueId.NIL);
  }

  /**
   * Get the runtime context.
   */
  public static RuntimeContext getRuntimeContext() {
    return runtime.getRuntimeContext();
  }
}
