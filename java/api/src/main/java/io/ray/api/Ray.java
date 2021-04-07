package io.ray.api;

import io.ray.api.id.PlacementGroupId;
import io.ray.api.id.UniqueId;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import io.ray.api.runtime.RayRuntime;
import io.ray.api.runtime.RayRuntimeFactory;
import io.ray.api.runtimecontext.RuntimeContext;
import java.util.List;
import java.util.Map;
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
   * Get a handle to a named actor of current job.
   *
   * <p>Gets a handle to a named actor with the given name. The actor must have been created with
   * name specified.
   *
   * @param name The name of the named actor.
   * @return an ActorHandle to the actor if the actor of specified name exists or an
   *     Optional.empty()
   */
  public static <T extends BaseActorHandle> Optional<T> getActor(String name) {
    return internal().getActor(name, false);
  }

  /**
   * Get a handle to a global named actor.
   *
   * <p>Gets a handle to a global named actor with the given name. The actor must have been created
   * with global name specified.
   *
   * @param name The global name of the named actor.
   * @return an ActorHandle to the actor if the actor of specified name exists or an
   *     Optional.empty()
   */
  public static <T extends BaseActorHandle> Optional<T> getGlobalActor(String name) {
    return internal().getActor(name, true);
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

  /**
   * Update the resource for the specified client. Set the resource for the specific node.
   *
   * @deprecated Consider using placement groups instead
   *     (docs.ray.io/en/master/placement-group.html). You can also specify resources at Ray start
   *     time with the 'resources' field in the cluster autoscaler.
   */
  @Deprecated
  public static void setResource(UniqueId nodeId, String resourceName, double capacity) {
    internal().setResource(resourceName, capacity, nodeId);
  }

  /**
   * Set the resource for local node.
   *
   * @deprecated Consider using placement groups instead
   *     (docs.ray.io/en/master/placement-group.html). You can also specify resources at Ray start
   *     time with the 'resources' field in the cluster autoscaler.
   */
  @Deprecated
  public static void setResource(String resourceName, double capacity) {
    internal().setResource(resourceName, capacity, UniqueId.NIL);
  }

  /** Get the runtime context. */
  public static RuntimeContext getRuntimeContext() {
    return internal().getRuntimeContext();
  }

  /**
   * Create a placement group. A placement group is used to place actors according to a specific
   * strategy and resource constraints. It will sends a request to GCS to preallocate the specified
   * resources, which is asynchronous. If the specified resource cannot be allocated, it will wait
   * for the resource to be updated and rescheduled.
   *
   * @deprecated This method is no longer recommended to create a new placement group, use {@link
   *     PlacementGroups#createPlacementGroup(PlacementGroupCreationOptions)} instead.
   * @param name Name of the placement group.
   * @param bundles Pre-allocated resource list.
   * @param strategy Actor placement strategy.
   * @return A handle to the created placement group.
   */
  @Deprecated
  public static PlacementGroup createPlacementGroup(
      String name, List<Map<String, Double>> bundles, PlacementStrategy strategy) {
    PlacementGroupCreationOptions creationOptions =
        new PlacementGroupCreationOptions.Builder()
            .setName(name)
            .setBundles(bundles)
            .setStrategy(strategy)
            .build();
    return PlacementGroups.createPlacementGroup(creationOptions);
  }

  /**
   * Create a placement group with an empty name.
   *
   * @deprecated This method is no longer recommended to create a new placement group, use {@link
   *     PlacementGroups#createPlacementGroup(PlacementGroupCreationOptions)} instead.
   */
  @Deprecated
  public static PlacementGroup createPlacementGroup(
      List<Map<String, Double>> bundles, PlacementStrategy strategy) {
    return createPlacementGroup(null, bundles, strategy);
  }

  /**
   * Create a placement group. A placement group is used to place actors according to a specific
   * strategy and resource constraints. It will sends a request to GCS to preallocate the specified
   * resources, which is asynchronous. If the specified resource cannot be allocated, it will wait
   * for the resource to be updated and rescheduled.
   *
   * @deprecated This method is no longer recommended to create a new placement group, use {@link
   *     PlacementGroups#createPlacementGroup(PlacementGroupCreationOptions)} instead.
   * @param creationOptions Creation options of the placement group.
   * @return A handle to the created placement group.
   */
  @Deprecated
  public static PlacementGroup createPlacementGroup(PlacementGroupCreationOptions creationOptions) {
    return PlacementGroups.createPlacementGroup(creationOptions);
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

  /**
   * Get a placement group by placement group Id.
   *
   * @deprecated This method is no longer recommended, use {@link
   *     PlacementGroups#getPlacementGroup(PlacementGroupId)} instead.
   * @param id placement group id.
   * @return The placement group.
   */
  @Deprecated
  public static PlacementGroup getPlacementGroup(PlacementGroupId id) {
    return PlacementGroups.getPlacementGroup(id);
  }

  /**
   * Get a placement group by placement group name from current job.
   *
   * @deprecated This method is no longer recommended to create a new placement group, use {@link
   *     PlacementGroups#getPlacementGroup(String)} instead.
   * @param name The placement group name.
   * @return The placement group.
   */
  @Deprecated
  public static PlacementGroup getPlacementGroup(String name) {
    return PlacementGroups.getPlacementGroup(name);
  }

  /**
   * Get a placement group by placement group name from all jobs.
   *
   * @deprecated This method is no longer recommended to create a new placement group, use {@link
   *     PlacementGroups#getGlobalPlacementGroup(String)} instead.
   * @param name The placement group name.
   * @return The placement group.
   */
  @Deprecated
  public static PlacementGroup getGlobalPlacementGroup(String name) {
    return PlacementGroups.getGlobalPlacementGroup(name);
  }

  /**
   * Get all placement groups in this cluster.
   *
   * @deprecated This method is no longer recommended to create a new placement group, use {@link
   *     PlacementGroups#getAllPlacementGroups()} instead.
   * @return All placement groups.
   */
  @Deprecated
  public static List<PlacementGroup> getAllPlacementGroups() {
    return PlacementGroups.getAllPlacementGroups();
  }

  /**
   * Remove a placement group by id. Throw RayException if remove failed.
   *
   * @param id Id of the placement group.
   * @deprecated This method is no longer recommended to create a new placement group, use {@link
   *     PlacementGroups#removePlacementGroup(PlacementGroupId)} instead.
   */
  public static void removePlacementGroup(PlacementGroupId id) {
    PlacementGroups.removePlacementGroup(id);
  }
}
