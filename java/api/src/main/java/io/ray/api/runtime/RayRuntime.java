package io.ray.api.runtime;

import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.WaitResult;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.api.function.PyFunction;
import io.ray.api.function.RayFunc;
import io.ray.api.id.ActorId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.id.UniqueId;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.CallOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import io.ray.api.runtimecontext.RuntimeContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/** Base interface of a Ray runtime. */
public interface RayRuntime {

  /** Shutdown the runtime. */
  void shutdown();

  /**
   * Store an object in the object store.
   *
   * @param obj The Java object to be stored.
   * @return A ObjectRef instance that represents the in-store object.
   */
  <T> ObjectRef<T> put(T obj);

  /**
   * Get an object from the object store.
   *
   * @param objectRef The reference of the object to get.
   * @return The Java object.
   */
  <T> T get(ObjectRef<T> objectRef);

  /**
   * Get a list of objects from the object store.
   *
   * @param objectRefs The list of object references.
   * @return A list of Java objects.
   */
  <T> List<T> get(List<ObjectRef<T>> objectRefs);

  /**
   * Wait for a list of RayObjects to be locally available, until specified number of objects are
   * ready, or specified timeout has passed.
   *
   * @param waitList A list of ObjectRef to wait for.
   * @param numReturns The number of objects that should be returned.
   * @param timeoutMs The maximum time in milliseconds to wait before returning.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  <T> WaitResult<T> wait(List<ObjectRef<T>> waitList, int numReturns, int timeoutMs);

  /**
   * Free a list of objects from Plasma Store.
   *
   * @param objectRefs The object references to free.
   * @param localOnly Whether only free objects for local object store or not.
   */
  void free(List<ObjectRef<?>> objectRefs, boolean localOnly);

  /**
   * Set the resource for the specific node.
   *
   * @param resourceName The name of resource.
   * @param capacity The capacity of the resource.
   * @param nodeId The node that we want to set its resource.
   */
  void setResource(String resourceName, double capacity, UniqueId nodeId);

  <T extends BaseActorHandle> T getActorHandle(ActorId actorId);

  /**
   * Get a handle to a named actor.
   *
   * <p>Gets a handle to a named actor with the given name. The actor must have been created with
   * name specified.
   *
   * @param name The name of the named actor.
   * @param global Whether the named actor is global.
   * @return ActorHandle to the actor.
   */
  <T extends BaseActorHandle> Optional<T> getActor(String name, boolean global);

  /**
   * Kill the actor immediately.
   *
   * @param actor The actor to be killed.
   * @param noRestart If set to true, the killed actor will not be restarted anymore.
   */
  void killActor(BaseActorHandle actor, boolean noRestart);

  /**
   * Invoke a remote function.
   *
   * @param func The remote function to run.
   * @param args The arguments of the remote function.
   * @param options The options for this call.
   * @return The result object.
   */
  ObjectRef call(RayFunc func, Object[] args, CallOptions options);

  /**
   * Invoke a remote Python function.
   *
   * @param pyFunction The Python function.
   * @param args Arguments of the function.
   * @param options The options for this call.
   * @return The result object.
   */
  ObjectRef call(PyFunction pyFunction, Object[] args, CallOptions options);

  /**
   * Invoke a remote function on an actor.
   *
   * @param actor A handle to the actor.
   * @param func The remote function to run, it must be a method of the given actor.
   * @param args The arguments of the remote function.
   * @return The result object.
   */
  ObjectRef callActor(ActorHandle<?> actor, RayFunc func, Object[] args);

  /**
   * Invoke a remote Python function on an actor.
   *
   * @param pyActor A handle to the actor.
   * @param pyActorMethod The actor method.
   * @param args Arguments of the function.
   * @return The result object.
   */
  ObjectRef callActor(PyActorHandle pyActor, PyActorMethod pyActorMethod, Object[] args);

  /**
   * Create an actor on a remote node.
   *
   * @param actorFactoryFunc A remote function whose return value is the actor object.
   * @param args The arguments for the remote function.
   * @param <T> The type of the actor object.
   * @param options The options for creating actor.
   * @return A handle to the actor.
   */
  <T> ActorHandle<T> createActor(
      RayFunc actorFactoryFunc, Object[] args, ActorCreationOptions options);

  /**
   * Create a Python actor on a remote node.
   *
   * @param pyActorClass The Python actor class.
   * @param args Arguments of the actor constructor.
   * @param options The options for creating actor.
   * @return A handle to the actor.
   */
  PyActorHandle createActor(PyActorClass pyActorClass, Object[] args, ActorCreationOptions options);

  PlacementGroup createPlacementGroup(
      String name, List<Map<String, Double>> bundles, PlacementStrategy strategy);

  PlacementGroup createPlacementGroup(
      List<Map<String, Double>> bundles, PlacementStrategy strategy);

  RuntimeContext getRuntimeContext();

  Object getAsyncContext();

  void setAsyncContext(Object asyncContext);

  /**
   * Wrap a {@link Runnable} with necessary context capture.
   *
   * @param runnable The runnable to wrap.
   * @return The wrapped runnable.
   */
  Runnable wrapRunnable(Runnable runnable);

  /**
   * Wrap a {@link Callable} with necessary context capture.
   *
   * @param callable The callable to wrap.
   * @return The wrapped callable.
   */
  <T> Callable<T> wrapCallable(Callable<T> callable);

  /** Intentionally exit the current actor. */
  void exitActor();

  /**
   * Get a placement group by id.
   *
   * @param id placement group id.
   * @return The placement group.
   */
  PlacementGroup getPlacementGroup(PlacementGroupId id);

  /**
   * Get all placement groups in this cluster.
   *
   * @return All placement groups.
   */
  List<PlacementGroup> getAllPlacementGroups();

  /**
   * Remove a placement group by id.
   *
   * @param id Id of the placement group.
   */
  void removePlacementGroup(PlacementGroupId id);

  /**
   * Wait for the placement group to be ready within the specified time.
   *
   * @param id Id of placement group.
   * @param timeoutMs Timeout in milliseconds.
   * @return True if the placement group is created. False otherwise.
   */
  boolean waitPlacementGroupReady(PlacementGroupId id, int timeoutMs);
}
