package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.ray.api.BaseActor;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.WaitResult;
import org.ray.api.exception.RayException;
import org.ray.api.function.PyActorClass;
import org.ray.api.function.PyActorMethod;
import org.ray.api.function.PyRemoteFunction;
import org.ray.api.function.RayFunc;
import org.ray.api.id.ObjectId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.api.runtimecontext.RuntimeContext;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.context.RuntimeContextImpl;
import org.ray.runtime.context.WorkerContext;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.functionmanager.PyFunctionDescriptor;
import org.ray.runtime.functionmanager.RayFunction;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.generated.Common.Language;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.object.ObjectStore;
import org.ray.runtime.object.RayObjectImpl;
import org.ray.runtime.task.ArgumentsBuilder;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.task.TaskExecutor;
import org.ray.runtime.task.TaskSubmitter;
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
    setIsContextSet(rayConfig.workerMode == WorkerType.DRIVER);
    functionManager = new FunctionManager(rayConfig.jobResourcePath);
    runtimeContext = new RuntimeContextImpl(this);
  }

  @Override
  public abstract void shutdown();

  @Override
  public <T> RayObject<T> put(T obj) {
    ObjectId objectId = objectStore.put(obj);
    return new RayObjectImpl<T>(objectId, (Class<T>)(obj == null ? Object.class : obj.getClass()));
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
  public <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs) {
    return objectStore.wait(waitList, numReturns, timeoutMs);
  }

  @Override
  public RayObject call(RayFunc func, Object[] args, CallOptions options) {
    RayFunction rayFunction = functionManager.getFunction(workerContext.getCurrentJobId(), func);
    FunctionDescriptor functionDescriptor = rayFunction.functionDescriptor;
    Optional<Class<?>> returnType = rayFunction.getReturnType();
    return callNormalFunction(functionDescriptor, args, returnType, options);
  }

  @Override
  public RayObject call(PyRemoteFunction pyRemoteFunction, Object[] args,
      CallOptions options) {
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(
        pyRemoteFunction.moduleName,
        "",
        pyRemoteFunction.functionName);
    // Python functions always have a return value, even if it's `None`.
    return callNormalFunction(functionDescriptor, args,
        /*returnType=*/Optional.of(pyRemoteFunction.returnType), options);
  }

  @Override
  public RayObject callActor(RayActor<?> actor, RayFunc func, Object[] args) {
    RayFunction rayFunction = functionManager.getFunction(workerContext.getCurrentJobId(), func);
    FunctionDescriptor functionDescriptor = rayFunction.functionDescriptor;
    Optional<Class<?>> returnType = rayFunction.getReturnType();
    return callActorFunction(actor, functionDescriptor, args, returnType);
  }

  @Override
  public RayObject callActor(RayPyActor pyActor, PyActorMethod pyActorMethod, Object... args) {
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(pyActor.getModuleName(),
        pyActor.getClassName(), pyActorMethod.methodName);
    // Python functions always have a return value, even if it's `None`.
    return callActorFunction(pyActor, functionDescriptor, args,
        /*returnType=*/Optional.of(pyActorMethod.returnType));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> RayActor<T> createActor(RayFunc actorFactoryFunc,
      Object[] args, ActorCreationOptions options) {
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(workerContext.getCurrentJobId(), actorFactoryFunc)
            .functionDescriptor;
    return (RayActor<T>) createActorImpl(functionDescriptor, args, options);
  }

  @Override
  public RayPyActor createActor(PyActorClass pyActorClass, Object[] args,
      ActorCreationOptions options) {
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(
        pyActorClass.moduleName,
        pyActorClass.className,
        PYTHON_INIT_METHOD_NAME);
    return (RayPyActor) createActorImpl(functionDescriptor, args, options);
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

  private RayObject callNormalFunction(FunctionDescriptor functionDescriptor,
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
      return new RayObjectImpl(returnIds.get(0), returnType.get());
    }
  }

  private RayObject callActorFunction(BaseActor rayActor,
      FunctionDescriptor functionDescriptor, Object[] args, Optional<Class<?>> returnType) {
    int numReturns = returnType.isPresent() ? 1 : 0;
    List<FunctionArg> functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage());
    List<ObjectId> returnIds = taskSubmitter.submitActorTask(rayActor,
        functionDescriptor, functionArgs, numReturns, null);
    Preconditions.checkState(returnIds.size() == numReturns);
    if (returnIds.isEmpty()) {
      return null;
    } else {
      return new RayObjectImpl(returnIds.get(0), returnType.get());
    }
  }

  private BaseActor createActorImpl(FunctionDescriptor functionDescriptor,
      Object[] args, ActorCreationOptions options) {
    List<FunctionArg> functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage());
    if (functionDescriptor.getLanguage() != Language.JAVA && options != null) {
      Preconditions.checkState(Strings.isNullOrEmpty(options.jvmOptions));
    }
    BaseActor actor = taskSubmitter.createActor(functionDescriptor, functionArgs, options);
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
