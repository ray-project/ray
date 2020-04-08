package io.ray.api.runtime;

import io.ray.api.BaseActor;
import io.ray.api.RayActor;
import io.ray.api.RayObject;
import io.ray.api.RayPyActor;
import io.ray.api.WaitResult;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.api.function.PyRemoteFunction;
import io.ray.api.function.RayFunc;
import io.ray.api.id.ObjectId;
import io.ray.api.id.UniqueId;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.CallOptions;
import io.ray.api.runtimecontext.RuntimeContext;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Base interface of a Ray runtime.
 */
public interface RayRuntime {

  /**
   * Shutdown the runtime.
   */
  void shutdown();

  /**
   * Store an object in the object store.
   *
   * @param obj The Java object to be stored.
   * @return A RayObject instance that represents the in-store object.
   */
  <T> RayObject<T> put(T obj);

  /**
   * Get an object from the object store.
   *
   * @param objectId The ID of the object to get.
   * @param objectType The type of the object to get.
   * @return The Java object.
   */
  <T> T get(ObjectId objectId, Class<T> objectType);

  /**
   * Get a list of objects from the object store.
   *
   * @param objectIds The list of object IDs.
   * @param objectType The type of object.
   * @return A list of Java objects.
   */
  <T> List<T> get(List<ObjectId> objectIds, Class<T> objectType);

  /**
   * Wait for a list of RayObjects to be locally available, until specified number of objects are
   * ready, or specified timeout has passed.
   *
   * @param waitList A list of RayObject to wait for.
   * @param numReturns The number of objects that should be returned.
   * @param timeoutMs The maximum time in milliseconds to wait before returning.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs);

  /**
   * Free a list of objects from Plasma Store.
   *
   * @param objectIds The object ids to free.
   * @param localOnly Whether only free objects for local object store or not.
   * @param deleteCreatingTasks Whether also delete objects' creating tasks from GCS.
   */
  void free(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks);

  /**
   * Set the resource for the specific node.
   *
   * @param resourceName The name of resource.
   * @param capacity The capacity of the resource.
   * @param nodeId The node that we want to set its resource.
   */
  void setResource(String resourceName, double capacity, UniqueId nodeId);

  /**
   * Kill the actor immediately.
   *
   * @param actor The actor to be killed.
   * @param noReconstruction If set to true, the killed actor will not be reconstructed anymore.
   */
  void killActor(BaseActor actor, boolean noReconstruction);

  /**
   * Invoke a remote function.
   *
   * @param func The remote function to run.
   * @param args The arguments of the remote function.
   * @param options The options for this call.
   * @return The result object.
   */
  RayObject call(RayFunc func, Object[] args, CallOptions options);

  /**
   * Invoke a remote Python function.
   *
   * @param pyRemoteFunction The Python function.
   * @param args Arguments of the function.
   * @param options The options for this call.
   * @return The result object.
   */
  RayObject call(PyRemoteFunction pyRemoteFunction, Object[] args, CallOptions options);

  /**
   * Invoke a remote function on an actor.
   *
   * @param actor A handle to the actor.
   * @param func The remote function to run, it must be a method of the given actor.
   * @param args The arguments of the remote function.
   * @return The result object.
   */
  RayObject callActor(RayActor<?> actor, RayFunc func, Object[] args);

  /**
   * Invoke a remote Python function on an actor.
   *
   * @param pyActor A handle to the actor.
   * @param pyActorMethod The actor method.
   * @param args Arguments of the function.
   * @return The result object.
   */
  RayObject callActor(RayPyActor pyActor, PyActorMethod pyActorMethod, Object[] args);

  /**
   * Create an actor on a remote node.
   *
   * @param actorFactoryFunc A remote function whose return value is the actor object.
   * @param args The arguments for the remote function.
   * @param <T> The type of the actor object.
   * @param options The options for creating actor.
   * @return A handle to the actor.
   */
  <T> RayActor<T> createActor(RayFunc actorFactoryFunc, Object[] args,
      ActorCreationOptions options);

  /**
   * Create a Python actor on a remote node.
   *
   * @param pyActorClass The Python actor class.
   * @param args Arguments of the actor constructor.
   * @param options The options for creating actor.
   * @return A handle to the actor.
   */
  RayPyActor createActor(PyActorClass pyActorClass, Object[] args,
                         ActorCreationOptions options);

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
}
