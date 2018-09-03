package org.ray.api.runtime;

import java.util.List;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.function.RayFunc;
import org.ray.api.id.UniqueId;

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
   * Wait for a list of RayObjects to be locally available,
   * until specified number of objects are ready, or specified timeout has passed.
   *
   * @param waitList A list of RayObject to wait for.
   * @param numReturns The number of objects that should be returned.
   * @param timeoutMs The maximum time in milliseconds to wait before returning.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs);

  /**
   * Create an actor on a remote node.
   *
   * @param actorClass the class of the actor to be created.
   * @return A handle to the newly created actor.
   */
  <T> RayActor<T> createActor(Class<T> actorClass);

  /**
   * Invoke a remote function.
   *
   * @param func The remote function to run.
   * @param args The arguments of the remote function.
   * @return The result object.
   */
  RayObject call(RayFunc func, Object[] args);

  /**
   * Invoke a remote function on an actor.
   *
   * @param func The remote function to run, it must be a method of the given actor.
   * @param actor A handle to the actor.
   * @param args The arguments of the remote function.
   * @return The result object.
   */
  RayObject call(RayFunc func, RayActor actor, Object[] args);
}
