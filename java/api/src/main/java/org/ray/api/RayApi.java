package org.ray.api;

import java.util.List;
import org.ray.api.funcs.RayFunc;
import org.ray.util.exception.TaskExecutionException;

/**
 * Ray runtime abstraction.
 */
public interface RayApi {

  /**
   * Put obj into object store.
   *
   * @return RayObject
   */
  <T> RayObject<T> put(T obj);

  <T, TMT> RayObject<T> put(T obj, TMT metadata);

  /**
   * Get real obj from object store.
   */
  <T> T get(UniqueID objectId) throws TaskExecutionException;

  /**
   * Get real objects from object store.
   *
   * @param objectIds list of ids of objects to get
   */
  <T> List<T> get(List<UniqueID> objectIds) throws TaskExecutionException;

  <T> T getMeta(UniqueID objectId) throws TaskExecutionException;

  <T> List<T> getMeta(List<UniqueID> objectIds) throws TaskExecutionException;

  /**
   * wait until timeout or enough RayObjects are ready.
   *
   * @param waitfor    wait for who
   * @param numReturns how many of ready is enough
   * @param timeout    in millisecond
   */
  <T> WaitResult<T> wait(List<RayObject<T>> waitfor, int numReturns, int timeout);

  /**
   * create remote actor.
   */
  <T> RayActor<T> create(Class<T> cls);

  /**
   * submit a new task by invoking a remote function.
   *
   * @param taskId      nil
   * @param funcCls     the target running function's class
   * @param lambda      the target running function
   * @param returnCount the number of to-be-returned objects from funcRun
   * @param args        arguments to this funcRun, can be its original form or RayObject
   * @return a set of ray objects with their return ids
   */
  RayObjects call(UniqueID taskId, Class<?> funcCls, RayFunc lambda, int returnCount,
                  Object... args);

}
