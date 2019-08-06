package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
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
import org.ray.api.runtimecontext.RuntimeContext;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.functionmanager.PyFunctionDescriptor;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.generated.Common.Language;
import org.ray.runtime.task.ArgumentsBuilder;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core functionality to implement Ray APIs.
 */
public abstract class AbstractRayRuntime implements RayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRayRuntime.class);
  protected RayConfig rayConfig;
  protected AbstractWorker worker;
  protected FunctionManager functionManager;
  protected RuntimeContext runtimeContext;
  protected GcsClient gcsClient;

  public AbstractRayRuntime(RayConfig rayConfig) {
    this.rayConfig = rayConfig;
    functionManager = new FunctionManager(rayConfig.jobResourcePath);
    runtimeContext = new RuntimeContextImpl(this);
  }

  /**
   * Start runtime.
   */
  public abstract void start() throws Exception;

  @Override
  public abstract void shutdown();

  @Override
  public <T> RayObject<T> put(T obj) {
    ObjectId objectId = worker.getObjectStoreProxy().put(obj);
    return new RayObjectImpl<>(objectId);
  }

  @Override
  public <T> T get(ObjectId objectId) throws RayException {
    List<T> ret = get(ImmutableList.of(objectId));
    return ret.get(0);
  }

  @Override
  public <T> List<T> get(List<ObjectId> objectIds) {
    return getWorker().getObjectStoreProxy().get(objectIds);
  }

  @Override
  public void free(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    worker.getObjectStoreProxy().delete(objectIds, localOnly, deleteCreatingTasks);
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    Preconditions.checkArgument(Double.compare(capacity, 0) >= 0);
    if (nodeId == null) {
      nodeId = UniqueId.NIL;
    }
    worker.getRayletClient().setResource(resourceName, capacity, nodeId);
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs) {
    Preconditions.checkNotNull(waitList);
    if (waitList.isEmpty()) {
      return new WaitResult<>(Collections.emptyList(), Collections.emptyList());
    }

    List<ObjectId> ids = waitList.stream().map(RayObject::getId).collect(Collectors.toList());

    List<Boolean> ready = worker.getObjectStoreProxy().wait(ids, numReturns, timeoutMs);
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
        functionManager.getFunction(worker.getWorkerContext().getCurrentJobId(), func)
            .functionDescriptor;
    return callNormalFunction(functionDescriptor, args, options);
  }

  @Override
  public RayObject call(RayFunc func, RayActor<?> actor, Object[] args) {
    AbstractRayActor abstractRayActor = (AbstractRayActor) actor;
    Preconditions.checkState(abstractRayActor.getLanguage() == Language.JAVA);
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(worker.getWorkerContext().getCurrentJobId(), func)
            .functionDescriptor;
    return callActorFunction(abstractRayActor, functionDescriptor, args);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> RayActor<T> createActor(RayFunc actorFactoryFunc,
      Object[] args, ActorCreationOptions options) {
    FunctionDescriptor functionDescriptor =
        functionManager.getFunction(worker.getWorkerContext().getCurrentJobId(), actorFactoryFunc)
            .functionDescriptor;
    return (RayActor<T>) createActorImpl(Language.JAVA, functionDescriptor, args, options);
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
  public RayObject callPy(String moduleName, String functionName, Object[] args,
      CallOptions options) {
    checkPyArguments(args);
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(moduleName, "",
        functionName);
    return callNormalFunction(functionDescriptor, args, options);
  }

  @Override
  public RayObject callPy(RayPyActor pyActor, String functionName, Object... args) {
    AbstractRayActor abstractRayActor = (AbstractRayActor) pyActor;
    Preconditions.checkState(abstractRayActor.getLanguage() == Language.PYTHON);
    checkPyArguments(args);
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(pyActor.getModuleName(),
        pyActor.getClassName(), functionName);
    return callActorFunction(abstractRayActor, functionDescriptor, args);
  }

  @Override
  public RayPyActor createPyActor(String moduleName, String className, Object[] args,
      ActorCreationOptions options) {
    checkPyArguments(args);
    PyFunctionDescriptor functionDescriptor = new PyFunctionDescriptor(moduleName, className,
        "__init__");
    return (RayPyActor) createActorImpl(Language.PYTHON, functionDescriptor, args, options);
  }

  private RayObject callNormalFunction(FunctionDescriptor functionDescriptor,
      Object[] args, CallOptions options) {
    FunctionArg[] functionArgs = ArgumentsBuilder
        .wrap(args, functionDescriptor.getLanguage() != Language.JAVA);
    List<ObjectId> returnIds = worker.getTaskInterface().submitTask(functionDescriptor,
        functionArgs, 1, options);
    return new RayObjectImpl(returnIds.get(0));
  }

  private RayObject callActorFunction(AbstractRayActor rayActor,
      FunctionDescriptor functionDescriptor, Object[] args) {
    Preconditions.checkState(rayActor.getLanguage() == functionDescriptor.getLanguage());
    FunctionArg[] functionArgs = ArgumentsBuilder
        .wrap(args, rayActor.getLanguage() != Language.JAVA);
    List<ObjectId> returnIds = worker.getTaskInterface().submitActorTask(rayActor,
        functionDescriptor, functionArgs, 1 /* core worker will plus it by 1, so put 1 here */,
        null);
    return new RayObjectImpl(returnIds.get(0));
  }

  private AbstractRayActor createActorImpl(Language language, FunctionDescriptor functionDescriptor,
      Object[] args, ActorCreationOptions options) {
    FunctionArg[] functionArgs = ArgumentsBuilder.wrap(args, language != Language.JAVA);
    if (language != Language.JAVA && options != null) {
      Preconditions.checkState(StringUtil.isNullOrEmpty(options.jvmOptions));
    }
    AbstractRayActor actor = (AbstractRayActor) worker.getTaskInterface()
        .createActor(functionDescriptor, functionArgs,
            options);
    Preconditions.checkState(actor.getLanguage() == language);
    return actor;
  }

  public AbstractWorker getWorker() {
    return worker;
  }

  public WorkerContext getWorkerContext() {
    return worker.getWorkerContext();
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
