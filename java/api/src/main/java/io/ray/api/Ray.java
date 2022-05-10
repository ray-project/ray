package io.ray.api;

import io.ray.api.runtime.RayRuntime;
import io.ray.api.runtime.RayRuntimeFactory;
import io.ray.api.runtimecontext.RuntimeContext;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/** This class contains all public APIs of Ray. */
public final class Ray extends RayCall {

  private static RayRuntime runtime = null;

  /** Initialize Ray runtime with the default runtime implementation. */
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
  private static synchronized void init(RayRuntimeFactory factory) {
    if (runtime == null) {
      runtime = factory.createRayRuntime();
      Runtime.getRuntime().addShutdownHook(new Thread(Ray::shutdown));
    }
  }

  /** Shutdown Ray runtime. */
  public static synchronized void shutdown() {
    if (runtime != null) {
      internal().shutdown();
      runtime = null;
    }
  }

  /**
   * Check if {@link #init} has been called yet.
   *
   * @return True if {@link #init} has already been called and false otherwise.
   */
  public static boolean isInitialized() {
    return runtime != null;
  }

  /**
   * Store an object in the object store.
   *
   * @param obj The Java object to be stored.
   * @return A ObjectRef instance that represents the in-store object.
   */
  public static <T> ObjectRef<T> put(T obj) {
    return internal().put(obj);
  }

  /**
   * Store an object in the object store and assign its ownership to owner. This function is
   * experimental.
   *
   * @param obj The Java object to be stored.
   * @param owner The actor that should own this object. This allows creating objects with lifetimes
   *     decoupled from that of the creating process. Note that the owner actor must be passed a
   *     reference to the object prior to the object creator exiting, otherwise the reference will
   *     still be lost.
   * @return A ObjectRef instance that represents the in-store object.
   */
  public static <T> ObjectRef<T> put(T obj, BaseActorHandle owner) {
    return internal().put(obj, owner);
  }

  /**
   * Get an object by `ObjectRef` from the object store.
   *
   * @param objectRef The reference of the object to get.
   * @param timeoutMs The maximum amount of time in miliseconds to wait before returning.
   * @return The Java object.
   * @throws RayTimeoutException If it's timeout to get the object.
   */
  public static <T> T get(ObjectRef<T> objectRef, long timeoutMs) {
    return internal().get(objectRef, timeoutMs);
  }

  /**
   * Get an object by `ObjectRef` from the object store.
   *
   * @param objectRef The reference of the object to get.
   * @return The Java object.
   */
  public static <T> T get(ObjectRef<T> objectRef) {
    return internal().get(objectRef);
  }

  /**
   * Get a list of objects by `ObjectRef`s from the object store.
   *
   * @param objectList A list of object references.
   * @param timeoutMs The maximum amount of time in miliseconds to wait before returning.
   * @return A list of Java objects.
   * @throws RayTimeoutException If it's timeout to get the object.
   */
  public static <T> List<T> get(List<ObjectRef<T>> objectList, long timeoutMs) {
    return internal().get(objectList, timeoutMs);
  }

  /**
   * Get a list of objects by `ObjectRef`s from the object store.
   *
   * @param objectList A list of object references.
   * @return A list of Java objects.
   */
  public static <T> List<T> get(List<ObjectRef<T>> objectList) {
    return internal().get(objectList);
  }

  /**
   * Wait for a list of RayObjects to be available, until specified number of objects are ready, or
   * specified timeout has passed.
   *
   * @param waitList A list of object references to wait for.
   * @param numReturns The number of objects that should be returned.
   * @param timeoutMs The maximum time in milliseconds to wait before returning.
   * @param fetchLocal If true, wait for the object to be downloaded onto the local node before
   *     returning it as ready. If false, ray.wait() will not trigger fetching of objects to the
   *     local node and will return immediately once the object is available anywhere in the
   *     cluster.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public static <T> WaitResult<T> wait(
      List<ObjectRef<T>> waitList, int numReturns, int timeoutMs, boolean fetchLocal) {
    return internal().wait(waitList, numReturns, timeoutMs, fetchLocal);
  }

  /**
   * Wait for a list of RayObjects to be locally available, until specified number of objects are
   * ready, or specified timeout has passed.
   *
   * @param waitList A list of object references to wait for.
   * @param numReturns The number of objects that should be returned.
   * @param timeoutMs The maximum time in milliseconds to wait before returning.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public static <T> WaitResult<T> wait(List<ObjectRef<T>> waitList, int numReturns, int timeoutMs) {
    return wait(waitList, numReturns, timeoutMs, true);
  }

  /**
   * Wait for a list of RayObjects to be locally available, until specified number of objects are
   * ready.
   *
   * @param waitList A list of object references to wait for.
   * @param numReturns The number of objects that should be returned.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public static <T> WaitResult<T> wait(List<ObjectRef<T>> waitList, int numReturns) {
    return wait(waitList, numReturns, Integer.MAX_VALUE);
  }

  /**
   * Wait for a list of RayObjects to be locally available.
   *
   * @param waitList A list of object references to wait for.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public static <T> WaitResult<T> wait(List<ObjectRef<T>> waitList) {
    return wait(waitList, waitList.size());
  }

  /**
   * Get a handle to a named actor in current namespace.
   *
   * <p>Gets a handle to a named actor with the given name of current namespace. The actor must have
   * been created with name specified.
   *
   * @param name The name of the named actor.
   * @return an ActorHandle to the actor if the actor of specified name exists in current namespace
   *     or an Optional.empty()
   * @throws RayException An exception is raised if timed out.
   */
  public static <T extends BaseActorHandle> Optional<T> getActor(String name) {
    return internal().getActor(name, null);
  }

  /**
   * Get a handle to a named actor in the given namespace.
   *
   * <p>Gets a handle to a named actor with the given name of the given namespace. The actor must
   * have been created with name specified.
   *
   * @param name The name of the named actor.
   * @param namespace The namespace of the actor.
   * @return an ActorHandle to the actor if the actor of specified name exists in current namespace
   *     or an Optional.empty()
   * @throws RayException An exception is raised if timed out.
   */
  public static <T extends BaseActorHandle> Optional<T> getActor(String name, String namespace) {
    return internal().getActor(name, namespace);
  }

  /**
   * If users want to use Ray API in their own threads, call this method to get the async context
   * and then call {@link #setAsyncContext} at the beginning of the new thread.
   *
   * @return The async context.
   */
  public static Object getAsyncContext() {
    return internal().getAsyncContext();
  }

  /**
   * Set the async context for the current thread.
   *
   * @param asyncContext The async context to set.
   */
  public static void setAsyncContext(Object asyncContext) {
    internal().setAsyncContext(asyncContext);
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
    return internal().wrapRunnable(runnable);
  }

  /**
   * If users want to use Ray API in their own threads, they should wrap their {@link Callable}
   * objects with this method.
   *
   * @param callable The callable to wrap.
   * @return The wrapped callable.
   */
  public static <T> Callable<T> wrapCallable(Callable<T> callable) {
    return internal().wrapCallable(callable);
  }

  /** Get the underlying runtime instance. */
  public static RayRuntime internal() {
    if (runtime == null) {
      throw new IllegalStateException(
          "Ray has not been started yet. You can start Ray with 'Ray.init()'");
    }
    return runtime;
  }

  /** Get the runtime context. */
  public static RuntimeContext getRuntimeContext() {
    return internal().getRuntimeContext();
  }

  /**
   * Intentionally exit the current actor.
   *
   * <p>This method is used to disconnect an actor and exit the worker.
   *
   * @throws RuntimeException An exception is raised if this is a driver or this worker is not an
   *     actor.
   */
  public static void exitActor() {
    runtime.exitActor();
  }
}
