package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.WaitResult;
import org.ray.api.exception.RayException;
import org.ray.api.function.RayFunc;
import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.BaseTaskOptions;
import org.ray.api.options.CallOptions;
import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtimecontext.RuntimeContext;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.functionmanager.PyFunctionDescriptor;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.RayletClient;
import org.ray.runtime.raylet.RayletClientImpl;
import org.ray.runtime.task.ArgumentsBuilder;
import org.ray.runtime.task.TaskLanguage;
import org.ray.runtime.task.TaskSpec;
import org.ray.runtime.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core functionality to implement Ray APIs.
 */
public abstract class AbstractRayRuntime implements RayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRayRuntime.class);

  protected RayConfig rayConfig;
  protected WorkerContext workerContext;
  protected Worker worker;
  protected RayletClient rayletClient;
  protected ObjectStoreProxy objectStoreProxy;
  protected FunctionManager functionManager;
  protected RuntimeContext runtimeContext;
  protected GcsClient gcsClient;

  static {
    try {
      LOGGER.debug("Loading native libraries.");
      // Load native libraries.
      String[] libraries = new String[]{"core_worker_library_java"};
      for (String library : libraries) {
        String fileName = System.mapLibraryName(library);
        // Copy the file from resources to a temp dir, and load the native library.
        File file = File.createTempFile(fileName, "");
        file.deleteOnExit();
        InputStream in = AbstractRayRuntime.class.getResourceAsStream("/" + fileName);
        Preconditions.checkNotNull(in, "{} doesn't exist.", fileName);
        Files.copy(in, Paths.get(file.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING);
        System.load(file.getAbsolutePath());
      }
      LOGGER.debug("Native libraries loaded.");
    } catch (IOException e) {
      throw new RuntimeException("Couldn't load native libraries.", e);
    }
  }

  public AbstractRayRuntime(RayConfig rayConfig) {
    this.rayConfig = rayConfig;
    functionManager = new FunctionManager(rayConfig.jobResourcePath);
    worker = new Worker(this);
    runtimeContext = new RuntimeContextImpl(this);
  }

  protected void resetLibraryPath() {
    if (rayConfig.libraryPath.isEmpty()) {
      return;
    }

    String path = System.getProperty("java.library.path");
    if (Strings.isNullOrEmpty(path)) {
      path = "";
    } else {
      path += ":";
    }
    path += String.join(":", rayConfig.libraryPath);

    // This is a hack to reset library path at runtime,
    // see https://stackoverflow.com/questions/15409223/.
    System.setProperty("java.library.path", path);
    // Set sys_paths to null so that java.library.path will be re-evaluated next time it is needed.
    final Field sysPathsField;
    try {
      sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
      sysPathsField.setAccessible(true);
      sysPathsField.set(null, null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      LOGGER.error("Failed to set library path.", e);
    }
  }

  /**
   * Start runtime.
   */
  public abstract void start() throws Exception;

  @Override
  public abstract void shutdown();

  @Override
  public <T> RayObject<T> put(T obj) {
    ObjectId objectId = ObjectId.forPut(workerContext.getCurrentTaskId(),
        workerContext.nextPutIndex());
    put(objectId, obj);
    return new RayObjectImpl<>(objectId);
  }

  public <T> void put(ObjectId objectId, T obj) {
    TaskId taskId = workerContext.getCurrentTaskId();
    LOGGER.debug("Putting object {}, for task {} ", objectId, taskId);
    objectStoreProxy.put(objectId, obj);
  }


  /**
   * Store a serialized object in the object store.
   *
   * @param obj The serialized Java object to be stored.
   * @return A RayObject instance that represents the in-store object.
   */
  public RayObject<Object> putSerialized(byte[] obj) {
    ObjectId objectId = ObjectId.forPut(workerContext.getCurrentTaskId(),
        workerContext.nextPutIndex());
    TaskId taskId = workerContext.getCurrentTaskId();
    LOGGER.debug("Putting serialized object {}, for task {} ", objectId, taskId);
    objectStoreProxy.putSerialized(objectId, obj);
    return new RayObjectImpl<>(objectId);
  }

  @Override
  public <T> T get(ObjectId objectId) throws RayException {
    List<T> ret = get(ImmutableList.of(objectId));
    return ret.get(0);
  }

  @Override
  public <T> List<T> get(List<ObjectId> objectIds) {
    return objectStoreProxy.get(objectIds);
  }

  @Override
  public void free(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    rayletClient.freePlasmaObjects(objectIds, localOnly, deleteCreatingTasks);
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    Preconditions.checkArgument(Double.compare(capacity, 0) >= 0);
    if (nodeId == null) {
      nodeId = UniqueId.NIL;
    }
    rayletClient.setResource(resourceName, capacity, nodeId);
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs) {
    return rayletClient.wait(waitList, numReturns,
        timeoutMs, workerContext.getCurrentTaskId());
  }

  @Override
  public RayObject call(RayFunc func, Object[] args, CallOptions options) {
    TaskSpec spec = createTaskSpec(func, null, RayActorImpl.NIL, args, false, false, options);
    rayletClient.submitTask(spec);
    return new RayObjectImpl(spec.returnIds[0]);
  }

  @Override
  public RayObject call(RayFunc func, RayActor<?> actor, Object[] args) {
    if (!(actor instanceof RayActorImpl)) {
      throw new IllegalArgumentException("Unsupported actor type: " + actor.getClass().getName());
    }
    RayActorImpl<?> actorImpl = (RayActorImpl) actor;
    TaskSpec spec;
    synchronized (actor) {
      spec = createTaskSpec(func, null, actorImpl, args, false, true, null);
      actorImpl.setTaskCursor(spec.returnIds[1]);
      actorImpl.clearNewActorHandles();
    }
    rayletClient.submitTask(spec);
    return new RayObjectImpl(spec.returnIds[0]);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> RayActor<T> createActor(RayFunc actorFactoryFunc,
      Object[] args, ActorCreationOptions options) {
    TaskSpec spec = createTaskSpec(actorFactoryFunc, null, RayActorImpl.NIL,
        args, true, false, options);
    RayActorImpl<?> actor = new RayActorImpl(spec.taskId.getActorId());
    actor.increaseTaskCounter();
    actor.setTaskCursor(spec.returnIds[0]);
    rayletClient.submitTask(spec);
    return (RayActor<T>) actor;
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
    PyFunctionDescriptor desc = new PyFunctionDescriptor(moduleName, "", functionName);
    TaskSpec spec = createTaskSpec(null, desc, RayPyActorImpl.NIL, args, false, false, options);
    rayletClient.submitTask(spec);
    return new RayObjectImpl(spec.returnIds[0]);
  }

  @Override
  public RayObject callPy(RayPyActor pyActor, String functionName, Object... args) {
    checkPyArguments(args);
    PyFunctionDescriptor desc = new PyFunctionDescriptor(pyActor.getModuleName(),
        pyActor.getClassName(), functionName);
    RayPyActorImpl actorImpl = (RayPyActorImpl) pyActor;
    TaskSpec spec;
    synchronized (pyActor) {
      spec = createTaskSpec(null, desc, actorImpl, args, false, true, null);
      actorImpl.setTaskCursor(spec.returnIds[1]);
      actorImpl.clearNewActorHandles();
    }
    rayletClient.submitTask(spec);
    return new RayObjectImpl(spec.returnIds[0]);
  }

  @Override
  public RayPyActor createPyActor(String moduleName, String className, Object[] args,
      ActorCreationOptions options) {
    checkPyArguments(args);
    PyFunctionDescriptor desc = new PyFunctionDescriptor(moduleName, className, "__init__");
    TaskSpec spec = createTaskSpec(null, desc, RayPyActorImpl.NIL, args, true, false, options);
    RayPyActorImpl actor = new RayPyActorImpl(spec.actorCreationId, moduleName, className);
    actor.increaseTaskCounter();
    actor.setTaskCursor(spec.returnIds[0]);
    rayletClient.submitTask(spec);
    return actor;
  }

  /**
   * Create the task specification.
   *
   * @param func The target remote function.
   * @param pyFunctionDescriptor Descriptor of the target Python function, if the task is a Python
   *                             task.
   * @param actor The actor handle. If the task is not an actor task, actor id must be NIL.
   * @param args The arguments for the remote function.
   * @param isActorCreationTask Whether this task is an actor creation task.
   * @param isActorTask Whether this task is an actor task.
   * @return A TaskSpec object.
   */
  private TaskSpec createTaskSpec(RayFunc func, PyFunctionDescriptor pyFunctionDescriptor,
      RayActorImpl<?> actor, Object[] args,
      boolean isActorCreationTask, boolean isActorTask, BaseTaskOptions taskOptions) {
    Preconditions.checkArgument((func == null) != (pyFunctionDescriptor == null));

    ActorId actorCreationId = ActorId.NIL;
    TaskId taskId = null;
    final JobId currentJobId = workerContext.getCurrentJobId();
    final TaskId currentTaskId = workerContext.getCurrentTaskId();
    final int taskIndex = workerContext.nextTaskIndex();
    if (isActorCreationTask) {
      taskId = RayletClientImpl.generateActorCreationTaskId(currentJobId, currentTaskId, taskIndex);
      actorCreationId = taskId.getActorId();
    } else if (isActorTask) {
      taskId = RayletClientImpl.generateActorTaskId(currentJobId, currentTaskId, taskIndex, actor.getId());
    } else {
      taskId = RayletClientImpl.generateNormalTaskId(currentJobId, currentTaskId, taskIndex);
    }

    int numReturns = actor.getId().isNil() ? 1 : 2;

    Map<String, Double> resources;
    if (null == taskOptions) {
      resources = new HashMap<>();
    } else {
      resources = new HashMap<>(taskOptions.resources);
    }

    int maxActorReconstruction = 0;
    List<String> dynamicWorkerOptions = ImmutableList.of();
    if (taskOptions instanceof ActorCreationOptions) {
      maxActorReconstruction = ((ActorCreationOptions) taskOptions).maxReconstructions;
      String jvmOptions = ((ActorCreationOptions) taskOptions).jvmOptions;
      if (!StringUtil.isNullOrEmpty(jvmOptions)) {
        dynamicWorkerOptions = ImmutableList.of(((ActorCreationOptions) taskOptions).jvmOptions);
      }
    }

    TaskLanguage language;
    FunctionDescriptor functionDescriptor;
    if (func != null) {
      language = TaskLanguage.JAVA;
      functionDescriptor = functionManager.getFunction(workerContext.getCurrentJobId(), func)
          .getFunctionDescriptor();
    } else {
      language = TaskLanguage.PYTHON;
      functionDescriptor = pyFunctionDescriptor;
    }

    ObjectId previousActorTaskDummyObjectId = ObjectId.NIL;
    if (isActorTask) {
      previousActorTaskDummyObjectId = actor.getTaskCursor();
    }

    return new TaskSpec(
        workerContext.getCurrentJobId(),
        taskId,
        workerContext.getCurrentTaskId(),
        -1,
        actorCreationId,
        maxActorReconstruction,
        actor.getId(),
        actor.getHandleId(),
        actor.increaseTaskCounter(),
        previousActorTaskDummyObjectId,
        actor.getNewActorHandles().toArray(new UniqueId[0]),
        ArgumentsBuilder.wrap(args, language == TaskLanguage.PYTHON),
        numReturns,
        resources,
        language,
        functionDescriptor,
        dynamicWorkerOptions
    );
  }

  public void loop() {
    worker.loop();
  }

  public Worker getWorker() {
    return worker;
  }

  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  public RayletClient getRayletClient() {
    return rayletClient;
  }

  public ObjectStoreProxy getObjectStoreProxy() {
    return objectStoreProxy;
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
