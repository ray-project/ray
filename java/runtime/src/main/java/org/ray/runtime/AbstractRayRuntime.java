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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.WaitResult;
import org.ray.api.exception.RayException;
import org.ray.api.function.RayFunc;
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
import org.ray.runtime.objectstore.ObjectStoreProxy.GetResult;
import org.ray.runtime.raylet.RayletClient;
import org.ray.runtime.task.ArgumentsBuilder;
import org.ray.runtime.task.TaskLanguage;
import org.ray.runtime.task.TaskSpec;
import org.ray.runtime.util.IdUtil;
import org.ray.runtime.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core functionality to implement Ray APIs.
 */
public abstract class AbstractRayRuntime implements RayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRayRuntime.class);

  /**
   * Default timeout of a get.
   */
  private static final int GET_TIMEOUT_MS = 1000;
  /**
   * Split objects in this batch size when fetching or reconstructing them.
   */
  private static final int FETCH_BATCH_SIZE = 1000;
  /**
   * Print a warning every this number of attempts.
   */
  private static final int WARN_PER_NUM_ATTEMPTS = 50;
  /**
   * Max number of ids to print in the warning message.
   */
  private static final int MAX_IDS_TO_PRINT_IN_WARNING = 20;

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
    ObjectId objectId = IdUtil.computePutId(
        workerContext.getCurrentTaskId(), workerContext.nextPutIndex());

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
    ObjectId objectId = IdUtil.computePutId(
        workerContext.getCurrentTaskId(), workerContext.nextPutIndex());
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
    List<T> ret = new ArrayList<>(Collections.nCopies(objectIds.size(), null));
    boolean wasBlocked = false;

    try {
      // A map that stores the unready object ids and their original indexes.
      Map<ObjectId, Integer> unready = new HashMap<>();
      for (int i = 0; i < objectIds.size(); i++) {
        unready.put(objectIds.get(i), i);
      }
      int numAttempts = 0;

      // Repeat until we get all objects.
      while (!unready.isEmpty()) {
        List<ObjectId> unreadyIds = new ArrayList<>(unready.keySet());

        // For the initial fetch, we only fetch the objects, do not reconstruct them.
        boolean fetchOnly = numAttempts == 0;
        if (!fetchOnly) {
          // If fetchOnly is false, this worker will be blocked.
          wasBlocked = true;
        }
        // Call `fetchOrReconstruct` in batches.
        for (List<ObjectId> batch : splitIntoBatches(unreadyIds)) {
          rayletClient.fetchOrReconstruct(batch, fetchOnly, workerContext.getCurrentTaskId());
        }

        // Get the objects from the object store, and parse the result.
        List<GetResult<T>> getResults = objectStoreProxy.get(unreadyIds, GET_TIMEOUT_MS);
        for (int i = 0; i < getResults.size(); i++) {
          GetResult<T> getResult = getResults.get(i);
          if (getResult.exists) {
            if (getResult.exception != null) {
              // If the result is an exception, throw it.
              throw getResult.exception;
            } else {
              // Set the result to the return list, and remove it from the unready map.
              ObjectId id = unreadyIds.get(i);
              ret.set(unready.get(id), getResult.object);
              unready.remove(id);
            }
          }
        }

        numAttempts += 1;
        if (LOGGER.isWarnEnabled() && numAttempts % WARN_PER_NUM_ATTEMPTS == 0) {
          // Print a warning if we've attempted too many times, but some objects are still
          // unavailable.
          List<ObjectId> idsToPrint = new ArrayList<>(unready.keySet());
          if (idsToPrint.size() > MAX_IDS_TO_PRINT_IN_WARNING) {
            idsToPrint = idsToPrint.subList(0, MAX_IDS_TO_PRINT_IN_WARNING);
          }
          String ids = idsToPrint.stream().map(ObjectId::toString)
              .collect(Collectors.joining(", "));
          if (idsToPrint.size() < unready.size()) {
            ids += ", etc";
          }
          String msg = String.format("Attempted %d times to reconstruct objects,"
                  + " but some objects are still unavailable. If this message continues to print,"
                  + " it may indicate that object's creating task is hanging, or something wrong"
                  + " happened in raylet backend. %d object(s) pending: %s.", numAttempts,
              unreadyIds.size(), ids);
          LOGGER.warn(msg);
        }
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Got objects {} for task {}.", Arrays.toString(objectIds.toArray()),
            workerContext.getCurrentTaskId());
      }

      return ret;
    } finally {
      // If there were objects that we weren't able to get locally, let the raylet backend
      // know that we're now unblocked.
      if (wasBlocked) {
        rayletClient.notifyUnblocked(workerContext.getCurrentTaskId());
      }
    }
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

  private List<List<ObjectId>> splitIntoBatches(List<ObjectId> objectIds) {
    List<List<ObjectId>> batches = new ArrayList<>();
    int objectsSize = objectIds.size();

    for (int i = 0; i < objectsSize; i += FETCH_BATCH_SIZE) {
      int endIndex = i + FETCH_BATCH_SIZE;
      List<ObjectId> batchIds = (endIndex < objectsSize)
          ? objectIds.subList(i, endIndex)
          : objectIds.subList(i, objectsSize);

      batches.add(batchIds);
    }

    return batches;
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
    RayActorImpl<?> actor = new RayActorImpl(new UniqueId(spec.returnIds[0].getBytes()));
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
   * task.
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

    TaskId taskId = rayletClient.generateTaskId(workerContext.getCurrentJobId(),
        workerContext.getCurrentTaskId(), workerContext.nextTaskIndex());
    int numReturns = actor.getId().isNil() ? 1 : 2;
    ObjectId[] returnIds = IdUtil.genReturnIds(taskId, numReturns);

    UniqueId actorCreationId = UniqueId.NIL;
    if (isActorCreationTask) {
      actorCreationId = new UniqueId(returnIds[0].getBytes());
    }

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
