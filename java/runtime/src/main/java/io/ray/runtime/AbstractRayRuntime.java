package io.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.WaitResult;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.api.function.PyFunction;
import io.ray.api.function.RayFunc;
import io.ray.api.function.RayFuncR;
import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.CallOptions;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.parallelactor.ParallelActorContext;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.runtime.RayRuntime;
import io.ray.api.runtimecontext.RuntimeContext;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.config.RunMode;
import io.ray.runtime.context.RuntimeContextImpl;
import io.ray.runtime.context.WorkerContext;
import io.ray.runtime.functionmanager.FunctionDescriptor;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.functionmanager.PyFunctionDescriptor;
import io.ray.runtime.functionmanager.RayFunction;
import io.ray.runtime.gcs.GcsClient;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.object.ObjectRefImpl;
import io.ray.runtime.object.ObjectStore;
import io.ray.runtime.runtimeenv.RuntimeEnvImpl;
import io.ray.runtime.task.ArgumentsBuilder;
import io.ray.runtime.task.FunctionArg;
import io.ray.runtime.task.TaskExecutor;
import io.ray.runtime.task.TaskSubmitter;
import io.ray.runtime.util.ConcurrencyGroupUtils;
import io.ray.runtime.utils.parallelactor.ParallelActorContextImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Core functionality to implement Ray APIs. */
public abstract class AbstractRayRuntime implements RayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRayRuntime.class);
  public static final String PYTHON_INIT_METHOD_NAME = "__init__";
  protected RayConfig rayConfig;
  protected TaskExecutor taskExecutor;
  protected FunctionManager functionManager;
  protected RuntimeContext runtimeContext;

  protected ObjectStore objectStore;
  protected TaskSubmitter taskSubmitter;
  protected WorkerContext workerContext;

  private static ParallelActorContextImpl parallelActorContextImpl = new ParallelActorContextImpl();

  public AbstractRayRuntime(RayConfig rayConfig) {
    this.rayConfig = rayConfig;
    runtimeContext = new RuntimeContextImpl(this);
  }

  @Override
  public <T> ObjectRef<T> put(T obj) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Putting Object in Task {}.", workerContext.getCurrentTaskId());
    }
    ObjectId objectId = objectStore.put(obj);
    return new ObjectRefImpl<T>(
        objectId,
        (Class<T>) (obj == null ? Object.class : obj.getClass()),
        /*skipAddingLocalRef=*/ true);
  }

  public abstract GcsClient getGcsClient();

  public abstract void start();

  public abstract void run();

  @Override
  public <T> ObjectRef<T> put(T obj, BaseActorHandle ownerActor) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Putting an object in task {} with {} as the owner.",
          workerContext.getCurrentTaskId(),
          ownerActor.getId());
    }
    ObjectId objectId = objectStore.put(obj, ownerActor.getId());
    return new ObjectRefImpl<T>(
        objectId,
        (Class<T>) (obj == null ? Object.class : obj.getClass()),
        /*skipAddingLocalRef=*/ true);
  }

  @Override
  public <T> T get(ObjectRef<T> objectRef) throws RuntimeException {
    return get(objectRef, -1);
  }

  @Override
  public <T> T get(ObjectRef<T> objectRef, long timeoutMs) throws RuntimeException {
    List<T> ret = get(ImmutableList.of(objectRef), timeoutMs);
    return ret.get(0);
  }

  @Override
  public <T> List<T> get(List<ObjectRef<T>> objectRefs) {
    return get(objectRefs, -1);
  }

  @Override
  public <T> List<T> get(List<ObjectRef<T>> objectRefs, long timeoutMs) {
    List<ObjectId> objectIds = new ArrayList<>();
    Class<T> objectType = null;
    for (ObjectRef<T> o : objectRefs) {
      ObjectRefImpl<T> objectRefImpl = (ObjectRefImpl<T>) o;
      objectIds.add(objectRefImpl.getId());
      objectType = objectRefImpl.getType();
    }
    LOGGER.debug("Getting Objects {}.", objectIds);
    return objectStore.get(objectIds, objectType, timeoutMs);
  }

  @Override
  public void free(List<ObjectRef<?>> objectRefs, boolean localOnly) {
    List<ObjectId> objectIds =
        objectRefs.stream()
            .map(ref -> ((ObjectRefImpl<?>) ref).getId())
            .collect(Collectors.toList());
    LOGGER.debug("Freeing Objects {}, localOnly = {}.", objectIds, localOnly);
    objectStore.delete(objectIds, localOnly);
  }

  @Override
  public <T> WaitResult<T> wait(
      List<ObjectRef<T>> waitList, int numReturns, int timeoutMs, boolean fetchLocal) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Waiting Objects {} with minimum number {} within {} ms.",
          waitList,
          numReturns,
          timeoutMs);
    }
    return objectStore.wait(waitList, numReturns, timeoutMs, fetchLocal);
  }

  @Override
  public ObjectRef call(RayFunc func, Object[] args, CallOptions options) {
    RayFunction rayFunction = functionManager.getFunction(func);
    FunctionDescriptor functionDescriptor = rayFunction.functionDescriptor;
    Optional<Class<?>> returnType = rayFunction.getReturnType();
    return callNormalFunction(functionDescriptor, args, returnType, options);
  }

  @Override
  public ObjectRef call(PyFunction pyFunction, Object[] args, CallOptions options) {
    PyFunctionDescriptor functionDescriptor =
        new PyFunctionDescriptor(pyFunction.moduleName, "", pyFunction.functionName);
    // Python functions always have a return value, even if it's `None`.
    return callNormalFunction(
        functionDescriptor, args, /*returnType=*/ Optional.of(pyFunction.returnType), options);
  }

  @Override
  public ObjectRef callActor(
      ActorHandle<?> actor, RayFunc func, Object[] args, CallOptions options) {
    RayFunction rayFunction = functionManager.getFunction(func);
    FunctionDescriptor functionDescriptor = rayFunction.functionDescriptor;
    Optional<Class<?>> returnType = rayFunction.getReturnType();
    return callActorFunction(actor, functionDescriptor, args, returnType, options);
  }

  @Override
  public ObjectRef callActor(PyActorHandle pyActor, PyActorMethod pyActorMethod, Object... args) {
    PyFunctionDescriptor functionDescriptor =
        new PyFunctionDescriptor(
            pyActor.getModuleName(), pyActor.getClassName(), pyActorMethod.methodName);
    // Python functions always have a return value, even if it's `None`.
    return callActorFunction(
        pyActor,
        functionDescriptor,
        args,
        /*returnType=*/ Optional.of(pyActorMethod.returnType),
        new CallOptions.Builder().build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ActorHandle<T> createActor(
      RayFunc actorFactoryFunc, Object[] args, ActorCreationOptions options) {
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(actorFactoryFunc).functionDescriptor;
    return (ActorHandle<T>) createActorImpl(functionDescriptor, args, options);
  }

  @Override
  public PyActorHandle createActor(
      PyActorClass pyActorClass, Object[] args, ActorCreationOptions options) {
    PyFunctionDescriptor functionDescriptor =
        new PyFunctionDescriptor(
            pyActorClass.moduleName, pyActorClass.className, PYTHON_INIT_METHOD_NAME);
    return (PyActorHandle) createActorImpl(functionDescriptor, args, options);
  }

  @Override
  public PlacementGroup createPlacementGroup(PlacementGroupCreationOptions creationOptions) {
    Preconditions.checkNotNull(
        creationOptions,
        "`PlacementGroupCreationOptions` must be specified when creating a new placement group.");
    return taskSubmitter.createPlacementGroup(creationOptions);
  }

  @Override
  public void removePlacementGroup(PlacementGroupId id) {
    taskSubmitter.removePlacementGroup(id);
  }

  @Override
  public PlacementGroup getPlacementGroup(PlacementGroupId id) {
    return getGcsClient().getPlacementGroupInfo(id);
  }

  @Override
  public PlacementGroup getPlacementGroup(String name, String namespace) {
    return namespace == null
        ? getGcsClient().getPlacementGroupInfo(name, runtimeContext.getNamespace())
        : getGcsClient().getPlacementGroupInfo(name, namespace);
  }

  @Override
  public List<PlacementGroup> getAllPlacementGroups() {
    return getGcsClient().getAllPlacementGroupInfo();
  }

  @Override
  public boolean waitPlacementGroupReady(PlacementGroupId id, int timeoutSeconds) {
    return taskSubmitter.waitPlacementGroupReady(id, timeoutSeconds);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends BaseActorHandle> T getActorHandle(ActorId actorId) {
    return (T) taskSubmitter.getActor(actorId);
  }

  @Override
  public ConcurrencyGroup createConcurrencyGroup(
      String name, int maxConcurrency, List<RayFunc> funcs) {
    return new ConcurrencyGroupImpl(name, maxConcurrency, funcs);
  }

  @Override
  public List<ConcurrencyGroup> extractConcurrencyGroups(RayFuncR<?> actorConstructorLambda) {
    return ConcurrencyGroupUtils.extractConcurrencyGroupsByAnnotations(actorConstructorLambda);
  }

  @Override
  public ParallelActorContext getParallelActorContext() {
    return parallelActorContextImpl;
  }

  @Override
  public RuntimeEnv createRuntimeEnv(Map<String, String> envVars, List<String> jars) {
    return new RuntimeEnvImpl(envVars, jars);
  }

  private ObjectRef callNormalFunction(
      FunctionDescriptor functionDescriptor,
      Object[] args,
      Optional<Class<?>> returnType,
      CallOptions options) {
    int numReturns = returnType.isPresent() ? 1 : 0;
    List<FunctionArg> functionArgs = ArgumentsBuilder.wrap(args, functionDescriptor.getLanguage());
    if (options == null) {
      options = new CallOptions.Builder().build();
    }

    ObjectRefImpl<?> impl = new ObjectRefImpl<>();
    /// Mapping the object id to the object ref.
    List<ObjectId> preparedReturnIds = getCurrentReturnIds(numReturns, ActorId.NIL);
    if (rayConfig.runMode == RunMode.CLUSTER && numReturns > 0) {
      ObjectRefImpl.registerObjectRefImpl(preparedReturnIds.get(0), impl);
    }

    List<ObjectId> returnIds =
        taskSubmitter.submitTask(functionDescriptor, functionArgs, numReturns, options);
    Preconditions.checkState(returnIds.size() == numReturns);
    validatePreparedReturnIds(preparedReturnIds, returnIds);
    if (returnIds.isEmpty()) {
      return null;
    } else {
      impl.init(returnIds.get(0), returnType.get(), /*skipAddingLocalRef=*/ true);
      return impl;
    }
  }

  private ObjectRef callActorFunction(
      BaseActorHandle rayActor,
      FunctionDescriptor functionDescriptor,
      Object[] args,
      Optional<Class<?>> returnType,
      CallOptions options) {
    int numReturns = returnType.isPresent() ? 1 : 0;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Submitting Actor Task {}.", functionDescriptor);
    }
    List<FunctionArg> functionArgs = ArgumentsBuilder.wrap(args, functionDescriptor.getLanguage());

    ObjectRefImpl<?> impl = new ObjectRefImpl<>();
    /// Mapping the object id to the object ref.
    List<ObjectId> preparedReturnIds = getCurrentReturnIds(numReturns, rayActor.getId());
    if (rayConfig.runMode == RunMode.CLUSTER && numReturns > 0) {
      ObjectRefImpl.registerObjectRefImpl(preparedReturnIds.get(0), impl);
    }
    List<ObjectId> returnIds =
        taskSubmitter.submitActorTask(
            rayActor, functionDescriptor, functionArgs, numReturns, options);
    Preconditions.checkState(returnIds.size() == numReturns);
    if (returnIds.isEmpty()) {
      return null;
    } else {
      validatePreparedReturnIds(preparedReturnIds, returnIds);
      impl.init(returnIds.get(0), returnType.get(), /*skipAddingLocalRef=*/ true);
      return impl;
    }
  }

  private BaseActorHandle createActorImpl(
      FunctionDescriptor functionDescriptor, Object[] args, ActorCreationOptions options) {
    if (LOGGER.isDebugEnabled()) {
      if (options == null) {
        LOGGER.debug("Creating Actor {} with default options.", functionDescriptor);
      } else {
        LOGGER.debug("Creating Actor {}, jvmOptions = {}.", functionDescriptor, options.jvmOptions);
      }
    }
    if (rayConfig.runMode == RunMode.LOCAL && functionDescriptor.getLanguage() != Language.JAVA) {
      throw new IllegalArgumentException(
          "Ray doesn't support cross-language invocation in local mode.");
    }

    List<FunctionArg> functionArgs = ArgumentsBuilder.wrap(args, functionDescriptor.getLanguage());
    if (functionDescriptor.getLanguage() != Language.JAVA && options != null) {
      Preconditions.checkState(options.jvmOptions == null || options.jvmOptions.size() == 0);
    }

    BaseActorHandle actor = taskSubmitter.createActor(functionDescriptor, functionArgs, options);
    return actor;
  }

  abstract List<ObjectId> getCurrentReturnIds(int numReturns, ActorId actorId);

  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  public ObjectStore getObjectStore() {
    return objectStore;
  }

  public TaskExecutor getTaskExecutor() {
    return taskExecutor;
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

  /// A helper to validate if the prepared return ids is as expected.
  void validatePreparedReturnIds(List<ObjectId> preparedReturnIds, List<ObjectId> realReturnIds) {
    if (rayConfig.runMode == RunMode.CLUSTER) {
      Preconditions.checkState(realReturnIds.size() == preparedReturnIds.size());
      for (int i = 0; i < preparedReturnIds.size(); ++i) {
        ObjectId prepared = preparedReturnIds.get(i);
        Object real = realReturnIds.get(i);
        Preconditions.checkState(
            prepared.equals(real),
            "The prepared object id {} is not equal to the real return id {}",
            prepared,
            real);
      }
    }
  }
}
