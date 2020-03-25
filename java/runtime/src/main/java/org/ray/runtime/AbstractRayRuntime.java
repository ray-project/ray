package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.List;
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
import org.ray.api.function.RayFuncVoid;
import org.ray.api.id.ObjectId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtimecontext.RuntimeContext;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.context.RuntimeContextImpl;
import org.ray.runtime.context.WorkerContext;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.functionmanager.PyFunctionDescriptor;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.generated.Common.Language;
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
public abstract class AbstractRayRuntime implements RayRuntime {

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

  public AbstractRayRuntime(RayConfig rayConfig, FunctionManager functionManager) {
    this.rayConfig = rayConfig;
    this.functionManager = functionManager;
    runtimeContext = new RuntimeContextImpl(this);
  }

  @Override
  public abstract void shutdown();

  @Override
  public <T> RayObject<T> put(T obj) {
    ObjectId objectId = objectStore.put(obj);
    return new RayObjectImpl<>(objectId);
  }

  @Override
  public <T> T get(ObjectId objectId) throws RayException {
    List<T> ret = get(ImmutableList.of(objectId));
    return ret.get(0);
  }

  @Override
  public <T> List<T> get(List<ObjectId> objectIds) {
    return objectStore.get(objectIds);
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
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(workerContext.getCurrentJobId(), func)
            .functionDescriptor;
    int numReturns = func instanceof RayFuncVoid ? 0 : 1;
    return callNormalFunction(functionDescriptor, args, numReturns, options);
  }

  @Override
  public RayObject call(PyRemoteFunction pyRemoteFunction, Object[] args,
                        CallOptions options) {
    checkPyArguments(args);
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(
        pyRemoteFunction.moduleName,
        "",
        pyRemoteFunction.functionName);
    // Python functions always have a return value, even if it's `None`.
    return callNormalFunction(functionDescriptor, args, /*numReturns=*/1, options);
  }

  @Override
  public RayObject callActor(RayActor<?> actor, RayFunc func, Object[] args) {
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(workerContext.getCurrentJobId(), func)
            .functionDescriptor;
    int numReturns = func instanceof RayFuncVoid ? 0 : 1;
    return callActorFunction(actor, functionDescriptor, args, numReturns);
  }

  @Override
  public RayObject callActor(RayPyActor pyActor, PyActorMethod pyActorMethod, Object... args) {
    checkPyArguments(args);
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(pyActor.getModuleName(),
        pyActor.getClassName(), pyActorMethod.methodName);
    // Python functions always have a return value, even if it's `None`.
    return callActorFunction(pyActor, functionDescriptor, args, /*numReturns=*/1);
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
    checkPyArguments(args);
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(
        pyActorClass.moduleName,
        pyActorClass.className,
        PYTHON_INIT_METHOD_NAME);
    return (RayPyActor) createActorImpl(functionDescriptor, args, options);
  }

  private void checkPyArguments(Object[] args) {
    for (Object arg : args) {
      Preconditions.checkArgument(
          (arg instanceof RayPyActor) || (arg instanceof byte[]),
          "Python argument can only be a RayPyActor or a byte array, not {}.",
          arg.getClass().getName());
    }
  }

  @Override
  public Runnable wrapRunnable(Runnable runnable) {
    return runnable;
  }

  @Override
  public Callable wrapCallable(Callable callable) {
    return callable;
  }

  private RayObject callNormalFunction(FunctionDescriptor functionDescriptor,
      Object[] args, int numReturns, CallOptions options) {
    List<FunctionArg> functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage());
    List<ObjectId> returnIds = taskSubmitter.submitTask(functionDescriptor,
        functionArgs, numReturns, options);
    Preconditions.checkState(returnIds.size() == numReturns && returnIds.size() <= 1);
    if (returnIds.isEmpty()) {
      return null;
    } else {
      return new RayObjectImpl(returnIds.get(0));
    }
  }

  private RayObject callActorFunction(BaseActor rayActor,
      FunctionDescriptor functionDescriptor, Object[] args, int numReturns) {
    List<FunctionArg> functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage());
    List<ObjectId> returnIds = taskSubmitter.submitActorTask(rayActor,
        functionDescriptor, functionArgs, numReturns, null);
    Preconditions.checkState(returnIds.size() == numReturns && returnIds.size() <= 1);
    if (returnIds.isEmpty()) {
      return null;
    } else {
      return new RayObjectImpl(returnIds.get(0));
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

  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  public ObjectStore getObjectStore() {
    return objectStore;
  }

  public FunctionManager getFunctionManager() {
    return functionManager;
  }

  public RayConfig getRayConfig() {
    return rayConfig;
  }

  public RuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  public GcsClient getGcsClient() {
    return gcsClient;
  }
}
