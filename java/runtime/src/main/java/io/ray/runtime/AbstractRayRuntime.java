package io.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.WaitResult;
import io.ray.api.exception.RayException;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.api.function.PyFunction;
import io.ray.api.function.RayFunc;
import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.CallOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import io.ray.api.runtimecontext.RuntimeContext;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.context.RuntimeContextImpl;
import io.ray.runtime.context.WorkerContext;
import io.ray.runtime.functionmanager.FunctionDescriptor;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.functionmanager.PyFunctionDescriptor;
import io.ray.runtime.functionmanager.RayFunction;
import io.ray.runtime.gcs.GcsClient;
import io.ray.runtime.generated.Common;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.object.ObjectRefImpl;
import io.ray.runtime.object.ObjectStore;
import io.ray.runtime.task.ArgumentsBuilder;
import io.ray.runtime.task.FunctionArg;
import io.ray.runtime.task.TaskExecutor;
import io.ray.runtime.task.TaskSubmitter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core functionality to implement Ray APIs.
 */
public abstract class AbstractRayRuntime implements RayRuntimeInternal {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRayRuntime.class);
  public static final String PYTHON_INIT_METHOD_NAME = "__init__";
  protected RayConfig rayConfig;
  protected TaskExecutor taskExecutor;
  protected FunctionManager functionManager;
  protected RuntimeContext runtimeContext;
  protected GcsClient gcsClient;

  protected ObjectStore objectStore;
  protected TaskSubmitter taskSubmitter;
  protected WorkerContext workerContext;

  /**
   * Whether the required thread context is set on the current thread.
   */
  final ThreadLocal<Boolean> isContextSet = ThreadLocal.withInitial(() -> false);

  public AbstractRayRuntime(RayConfig rayConfig) {
    this.rayConfig = rayConfig;
    setIsContextSet(rayConfig.workerMode == Common.WorkerType.DRIVER);
    functionManager = new FunctionManager(rayConfig.jobResourcePath);
    runtimeContext = new RuntimeContextImpl(this);
  }

  @Override
  public abstract void shutdown();

  @Override
  public <T> ObjectRef<T> put(T obj) {
    ObjectId objectId = objectStore.put(obj);
    return new ObjectRefImpl<T>(objectId, (Class<T>)(obj == null ? Object.class : obj.getClass()));
  }

  @Override
  public <T> T get(ObjectId objectId, Class<T> objectType) throws RayException {
    List<T> ret = get(ImmutableList.of(objectId), objectType);
    return ret.get(0);
  }

  @Override
  public <T> List<T> get(List<ObjectId> objectIds, Class<T> objectType) {
    return objectStore.get(objectIds, objectType);
  }

  @Override
  public void free(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    objectStore.delete(objectIds, localOnly, deleteCreatingTasks);
  }

  @Override
  public <T> WaitResult<T> wait(List<ObjectRef<T>> waitList, int numReturns, int timeoutMs) {
    return objectStore.wait(waitList, numReturns, timeoutMs);
  }

  @Override
  public ObjectRef call(RayFunc func, Object[] args, CallOptions options) {
    RayFunction rayFunction = functionManager.getFunction(workerContext.getCurrentJobId(), func);
    FunctionDescriptor functionDescriptor = rayFunction.functionDescriptor;
    Optional<Class<?>> returnType = rayFunction.getReturnType();
    return callNormalFunction(functionDescriptor, args, returnType, options);
  }

  @Override
  public ObjectRef call(PyFunction pyFunction, Object[] args, CallOptions options) {
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(
        pyFunction.moduleName,
        "",
        pyFunction.functionName);
    // Python functions always have a return value, even if it's `None`.
    return callNormalFunction(functionDescriptor, args,
        /*returnType=*/Optional.of(pyFunction.returnType), options);
  }

  @Override
  public ObjectRef callActor(ActorHandle<?> actor, RayFunc func, Object[] args) {
    RayFunction rayFunction = functionManager.getFunction(workerContext.getCurrentJobId(), func);
    FunctionDescriptor functionDescriptor = rayFunction.functionDescriptor;
    Optional<Class<?>> returnType = rayFunction.getReturnType();
    return callActorFunction(actor, functionDescriptor, args, returnType);
  }

  @Override
  public ObjectRef callActor(PyActorHandle pyActor, PyActorMethod pyActorMethod, Object... args) {
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(pyActor.getModuleName(),
        pyActor.getClassName(), pyActorMethod.methodName);
    // Python functions always have a return value, even if it's `None`.
    return callActorFunction(pyActor, functionDescriptor, args,
        /*returnType=*/Optional.of(pyActorMethod.returnType));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ActorHandle<T> createActor(RayFunc actorFactoryFunc,
                                        Object[] args, ActorCreationOptions options) {
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(workerContext.getCurrentJobId(), actorFactoryFunc)
            .functionDescriptor;
    return (ActorHandle<T>) createActorImpl(functionDescriptor, args, options);
  }

  @Override
  public PyActorHandle createActor(PyActorClass pyActorClass, Object[] args,
                                   ActorCreationOptions options) {
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(
        pyActorClass.moduleName,
        pyActorClass.className,
        PYTHON_INIT_METHOD_NAME);
    return (PyActorHandle) createActorImpl(functionDescriptor, args, options);
  }

  @Override
  public PlacementGroup createPlacementGroup(List<Map<String, Double>> bundles,
      PlacementStrategy strategy) {
    return taskSubmitter.createPlacementGroup(bundles, strategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends BaseActorHandle> T getActorHandle(ActorId actorId) {
    return (T) taskSubmitter.getActor(actorId);
  }

  @Override
  public void setAsyncContext(Object asyncContext) {
    isContextSet.set(true);
  }

  // TODO (kfstorm): Simplify the duplicate code in wrap*** methods.

  @Override
  public final Runnable wrapRunnable(Runnable runnable) {
    Object asyncContext = getAsyncContext();
    return () -> {
      boolean oldIsContextSet = isContextSet.get();
      Object oldAsyncContext = null;
      if (oldIsContextSet) {
        oldAsyncContext = getAsyncContext();
      }
      setAsyncContext(asyncContext);
      try {
        runnable.run();
      } finally {
        if (oldIsContextSet) {
          setAsyncContext(oldAsyncContext);
        } else {
          setIsContextSet(false);
        }
      }
    };
  }

  @Override
  public final <T> Callable<T> wrapCallable(Callable<T> callable) {
    Object asyncContext = getAsyncContext();
    return () -> {
      boolean oldIsContextSet = isContextSet.get();
      Object oldAsyncContext = null;
      if (oldIsContextSet) {
        oldAsyncContext = getAsyncContext();
      }
      setAsyncContext(asyncContext);
      try {
        return callable.call();
      } finally {
        if (oldIsContextSet) {
          setAsyncContext(oldAsyncContext);
        } else {
          setIsContextSet(false);
        }
      }
    };
  }

  private ObjectRef callNormalFunction(
      FunctionDescriptor functionDescriptor,
      Object[] args, Optional<Class<?>> returnType, CallOptions options) {
    int numReturns = returnType.isPresent() ? 1 : 0;
    List<FunctionArg> functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage());
    List<ObjectId> returnIds = taskSubmitter.submitTask(functionDescriptor,
        functionArgs, numReturns, options);
    Preconditions.checkState(returnIds.size() == numReturns);
    if (returnIds.isEmpty()) {
      return null;
    } else {
      return new ObjectRefImpl(returnIds.get(0), returnType.get());
    }
  }

  private ObjectRef callActorFunction(
      BaseActorHandle rayActor,
      FunctionDescriptor functionDescriptor,
      Object[] args,
      Optional<Class<?>> returnType) {
    int numReturns = returnType.isPresent() ? 1 : 0;
    List<FunctionArg> functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage());
    List<ObjectId> returnIds = taskSubmitter.submitActorTask(rayActor,
        functionDescriptor, functionArgs, numReturns, null);
    Preconditions.checkState(returnIds.size() == numReturns);
    if (returnIds.isEmpty()) {
      return null;
    } else {
      return new ObjectRefImpl(returnIds.get(0), returnType.get());
    }
  }

  private BaseActorHandle createActorImpl(FunctionDescriptor functionDescriptor,
                                          Object[] args, ActorCreationOptions options) {
    List<FunctionArg> functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage());
    if (functionDescriptor.getLanguage() != Language.JAVA && options != null) {
      Preconditions.checkState(Strings.isNullOrEmpty(options.jvmOptions));
    }
    BaseActorHandle actor = taskSubmitter.createActor(functionDescriptor, functionArgs, options);
    return actor;
  }

  @Override
  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  @Override
  public ObjectStore getObjectStore() {
    return objectStore;
  }

  @Override
  public FunctionManager getFunctionManager() {
    return functionManager;
  }

  @Override
  public RayConfig getRayConfig() {
    return rayConfig;
  }

  public RuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  @Override
  public GcsClient getGcsClient() {
    return gcsClient;
  }

  @Override
  public void setIsContextSet(boolean isContextSet) {
    this.isContextSet.set(isContextSet);
  }
}
