package org.ray.api;

import java.util.List;
import org.ray.api.function.RayFunc;

/**
 * Ray runtime abstraction.
 */
public interface RayApi {

  <T> RayObject<T> put(T obj);

  <T> T get(UniqueID objectId);

  <T> List<T> get(List<UniqueID> objectId);

  <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs);

  <T> RayActor<T> createActor(Class<T> cls);

  /**
   * Invoke a remote function.
   *
   * @param func the remote function to run.
   * @param args arguments of the remote function.
   * @return a set of ray objects with their return ids
   */
  RayObject call(RayFunc func, Object[] args);

  RayObject call(RayFunc func, RayActor actor, Object[] args);
}
