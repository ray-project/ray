package org.ray.api;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.ray.api.internal.Callable;
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
  <T> WaitResult<T> wait(RayList<T> waitfor, int numReturns, int timeout);

  /**
   * create remote actor.
   */
  <T> RayActor<T> create(Class<T> cls);

  /**
   * submit a new task by invoking a remote function.
   *
   * @param taskId      nil
   * @param funcRun     the target running function with @RayRemote
   * @param returnCount the number of to-be-returned objects from funcRun
   * @param args        arguments to this funcRun, can be its original form or RayObject
   * @return a set of ray objects with their return ids
   */
  RayObjects call(UniqueID taskId, Callable funcRun, int returnCount, Object... args);

  RayObjects call(UniqueID taskId, Class<?> funcCls, Serializable lambda, int returnCount,
                  Object... args);

  /**
   * In some cases, we would like the return value of a remote function to be splitted into multiple
   * parts so that they are consumed by multiple further functions separately (potentially on
   * different machines). We therefore introduce this API so that developers can annotate the
   * outputs with a set of labels (usually with Integer or String).
   *
   * @param taskId    nil
   * @param funcRun   the target running function with @RayRemote
   * @param returnIds a set of labels to be used by the returned objects
   * @param args      arguments to this funcRun, can be its original form or
   *                  RayObject<original-type>
   * @return a set of ray objects with their labels and return ids
   */
  <R, RIDT> RayMap<RIDT, R> callWithReturnLabels(UniqueID taskId, Callable funcRun,
                                                 Collection<RIDT> returnIds, Object... args);

  <R, RIDT> RayMap<RIDT, R> callWithReturnLabels(UniqueID taskId, Class<?> funcCls,
                                                 Serializable lambda, Collection<RIDT> returnids,
                                                 Object... args);

  /**
   * a special case for the above RID-based labeling as <0...returnCount - 1>.
   *
   * @param taskId      nil
   * @param funcRun     the target running function with @RayRemote
   * @param returnCount the number of to-be-returned objects from funcRun
   * @param args        arguments to this funcRun, can be its original form or
   *                    RayObject<original-type>
   * @return an array of returned objects with their Unique ids
   */
  <R> RayList<R> callWithReturnIndices(UniqueID taskId, Callable funcRun, Integer returnCount,
                                       Object... args);

  <R> RayList<R> callWithReturnIndices(UniqueID taskId, Class<?> funcCls, Serializable lambda,
                                       Integer returnCount, Object... args);

  boolean isRemoteLambda();
}
