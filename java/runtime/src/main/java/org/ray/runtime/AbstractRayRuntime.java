package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.WaitResult;
import org.ray.api.exception.RayException;
import org.ray.api.function.RayFunc;
import org.ray.api.id.ObjectId;
import org.ray.api.id.UniqueId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.task.ArgumentsBuilder;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.util.Serializer;

/**
 * Core functionality to implement Ray APIs.
 */
public abstract class AbstractRayRuntime implements RayRuntime {
  protected RayConfig rayConfig;
  protected Worker worker;
  protected FunctionManager functionManager;

  public AbstractRayRuntime(RayConfig rayConfig) {
    this.rayConfig = rayConfig;
    functionManager = new FunctionManager(rayConfig.driverResourcePath);
  }

  /**
   * Start runtime.
   */
  public abstract void start() throws Exception;

  @Override
  public abstract void shutdown();

  public RayConfig getRayConfig() {
    return rayConfig;
  }

  @Override
  public <T> RayObject<T> put(T obj) {
    ObjectId objectId = worker.getObjectInterface().put(obj);
    return new RayObjectImpl<>(objectId);
  }

  public <T> void put(ObjectId objectId, T obj) {
//    TaskId taskId = workerContext.getCurrentTaskId();
//    LOGGER.debug("Putting object {}, for task {} ", objectId, taskId);
    worker.getObjectInterface().put(objectId, obj);
  }


  /**
   * Store a serialized object in the object store.
   *
   * @param obj The serialized Java object to be stored.
   * @return A RayObject instance that represents the in-store object.
   */
  public RayObject<Object> putSerialized(byte[] obj) {
//    TaskId taskId = workerContext.getCurrentTaskId();
//    LOGGER.debug("Putting serialized object {}, for task {} ", objectId, taskId);
    ObjectId objectId = worker.getObjectInterface().putSerialized(obj);
    return new RayObjectImpl<>(objectId);
  }

  @Override
  public <T> T get(ObjectId objectId) throws RayException {
    List<T> ret = get(ImmutableList.of(objectId));
    return ret.get(0);
  }

  @Override
  public <T> List<T> get(List<ObjectId> objectIds) {
    // TODO: how to handle exception in get result without wait for all objects
    // TODO: how to pass timeoutMs to wait infinitely?
    // TODO: how to support deserializeFromMeta?
    List<byte[]> binaryResults = worker.getObjectInterface().get(objectIds, Long.MAX_VALUE);
    return binaryResults.stream().map(binaryResult -> {
          Object obj =
              Serializer.decode(binaryResult, worker.getCurrentClassLoader());
          if (obj instanceof RayException) {
            throw (RayException) obj;
          } else {
            //noinspection unchecked
            return (T) obj;
          }
        }
    ).collect(Collectors.toList());
  }

  @Override
  public void free(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    worker.getObjectInterface().delete(objectIds, localOnly, deleteCreatingTasks);
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    throw new UnsupportedOperationException();
//    Preconditions.checkArgument(Double.compare(capacity, 0) >= 0);
//    if (nodeId == null) {
//      nodeId = UniqueId.NIL;
//    }
//    rayletClient.setResource(resourceName, capacity, nodeId);
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs) {
    Preconditions.checkNotNull(waitList);
    if (waitList.isEmpty()) {
      return new WaitResult<>(new ArrayList<>(), new ArrayList<>());
    }

    List<ObjectId> ids = waitList.stream().map(RayObject::getId).collect(Collectors.toList());

    List<Boolean> ready = worker.getObjectInterface().wait(ids, numReturns, timeoutMs);
    List<RayObject<T>> readyList = new ArrayList<>();
    List<RayObject<T>> unreadyList = new ArrayList<>();

    for (int i = 0; i < ready.size(); i++) {
      if (ready.get(i)) {
        readyList.add(waitList.get(i));
      } else {
        unreadyList.add(waitList.get(i));
      }
    }

    return new WaitResult<>(readyList, unreadyList);
  }

  @Override
  public RayObject call(RayFunc func, Object[] args, CallOptions options) {
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(worker.getCurrentDriverId(), func).functionDescriptor;
    FunctionArg[] functionArgs = ArgumentsBuilder.wrap(args, false);
    int numReturns = getNumReturns(null);
    List<ObjectId> returnIds = worker.getTaskInterface().submitTask(functionDescriptor,
        functionArgs, numReturns, options);
    return new RayObjectImpl(returnIds.get(0));
  }

  @Override
  public RayObject call(RayFunc func, RayActor<?> actor, Object[] args) {
    if (!(actor instanceof RayActorImpl)) {
      throw new IllegalArgumentException("Unsupported actor type: " + actor.getClass().getName());
    }
    RayActorImpl<?> actorImpl = (RayActorImpl) actor;
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(worker.getCurrentDriverId(), func).functionDescriptor;
    FunctionArg[] functionArgs = ArgumentsBuilder.wrap(args, false);
    int numReturns = getNumReturns(actor);
    List<ObjectId> returnIds = worker.getTaskInterface().submitActorTask(actorImpl,
        functionDescriptor, functionArgs, numReturns, null);
    return new RayObjectImpl(returnIds.get(0));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> RayActor<T> createActor(RayFunc actorFactoryFunc,
                                     Object[] args, ActorCreationOptions options) {
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(worker.getCurrentDriverId(), actorFactoryFunc).functionDescriptor;
    FunctionArg[] functionArgs = ArgumentsBuilder.wrap(args, false);
    return (RayActor<T>) worker.getTaskInterface().createActor(functionDescriptor, functionArgs,
        options);
  }

//  private void checkPyArguments(Object[] args) {
//    for (Object arg : args) {
//      Preconditions.checkArgument(
//          (arg instanceof RayPyActor) || (arg instanceof byte[]),
//          "Python argument can only be a RayPyActor or a byte array, not {}.",
//          arg.getClass().getName());
//    }
//  }

  @Override
  public RayObject callPy(String moduleName, String functionName, Object[] args,
                          CallOptions options) {
    throw new UnsupportedOperationException();
//    checkPyArguments(args);
//    PyFunctionDescriptor desc = new PyFunctionDescriptor(moduleName, "", functionName);
//    TaskSpec spec = createTaskSpec(null, desc, RayPyActorImpl.NIL, args, false, options);
//    rayletClient.submitTask(spec);
//    return new RayObjectImpl(spec.returnIds[0]);
  }

  @Override
  public RayObject callPy(RayPyActor pyActor, String functionName, Object... args) {
    throw new UnsupportedOperationException();
//    checkPyArguments(args);
//    PyFunctionDescriptor desc = new PyFunctionDescriptor(pyActor.getModuleName(),
//        pyActor.getClassName(), functionName);
//    RayPyActorImpl actorImpl = (RayPyActorImpl) pyActor;
//    TaskSpec spec;
//    synchronized (pyActor) {
//      spec = createTaskSpec(null, desc, actorImpl, args, false, null);
//      spec.getExecutionDependencies().add(actorImpl.getTaskCursor());
//      actorImpl.setTaskCursor(spec.returnIds[1]);
//      actorImpl.clearNewActorHandles();
//    }
//    rayletClient.submitTask(spec);
//    return new RayObjectImpl(spec.returnIds[0]);
  }

  @Override
  public RayPyActor createPyActor(String moduleName, String className, Object[] args,
                                  ActorCreationOptions options) {
    throw new UnsupportedOperationException();
//    checkPyArguments(args);
//    PyFunctionDescriptor desc = new PyFunctionDescriptor(moduleName, className, "__init__");
//    TaskSpec spec = createTaskSpec(null, desc, RayPyActorImpl.NIL, args, true, options);
//    RayPyActorImpl actor = new RayPyActorImpl(spec.actorCreationId, moduleName, className);
//    actor.increaseTaskCounter();
//    actor.setTaskCursor(spec.returnIds[0]);
//    rayletClient.submitTask(spec);
//    return actor;
  }

  private int getNumReturns(RayActor<?> actor) {
    return actor == null || actor.getId().isNil() ? 1 : 2;
  }

  public void loop() {
    worker.loop();
  }
}
