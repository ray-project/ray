package org.ray.api.runtime;

import java.util.List;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.WaitResult;
import org.ray.api.function.RayFunc;
import org.ray.api.id.UniqueId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.api.runtimecontext.RuntimeContext;

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
   * @return The Java object.
   */
  <T> T get(UniqueId objectId);

  /**
   * Get a list of objects from the object store.
   *
   * @param objectIds The list of object IDs.
   * @return A list of Java objects.
   */
  <T> List<T> get(List<UniqueId> objectIds);

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
  void free(List<UniqueId> objectIds, boolean localOnly, boolean deleteCreatingTasks);

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
   * Invoke a remote function on an actor.
   *
   * @param func The remote function to run, it must be a method of the given actor.
   * @param actor A handle to the actor.
   * @param args The arguments of the remote function.
   * @return The result object.
   */
  RayObject call(RayFunc func, RayActor<?> actor, Object[] args);

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

  RuntimeContext getRuntimeContext();

  /**
   * Invoke a remote Python function.
   *
   * @param moduleName Module name of the Python function.
   * @param functionName Name of the Python function.
   * @param args Arguments of the function.
   * @param options The options for this call.
   * @return The result object.
   */
  RayObject callPy(String moduleName, String functionName, Object[] args, CallOptions options);

  /**
   * Invoke a remote Python function on an actor.
   *
   * @param pyActor A handle to the actor.
   * @param functionName Name of the actor method.
   * @param args Arguments of the function.
   * @return The result object.
   */
  RayObject callPy(RayPyActor pyActor, String functionName, Object[] args);

  /**
   * Create a Python actor on a remote node.
   *
   * @param moduleName Module name of the Python actor class.
   * @param className Name of the Python actor class.
   * @param args Arguments of the actor constructor.
   * @param options The options for creating actor.
   * @return A handle to the actor.
   */
  RayPyActor createPyActor(String moduleName, String className, Object[] args,
      ActorCreationOptions options);
}
