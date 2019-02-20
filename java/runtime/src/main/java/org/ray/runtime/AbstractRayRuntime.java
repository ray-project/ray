package org.ray.runtime;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.exception.RayException;
import org.ray.api.function.RayFunc;
import org.ray.api.id.UniqueId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.BaseTaskOptions;
import org.ray.api.options.CallOptions;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.functionmanager.RayFunction;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.objectstore.ObjectStoreProxy.GetResult;
import org.ray.runtime.raylet.RayletClient;
import org.ray.runtime.task.ArgumentsBuilder;
import org.ray.runtime.task.TaskSpec;
import org.ray.runtime.util.ResourceUtil;
import org.ray.runtime.util.UniqueIdUtil;
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

  public AbstractRayRuntime(RayConfig rayConfig) {
    this.rayConfig = rayConfig;
    functionManager = new FunctionManager(rayConfig.driverResourcePath);
    worker = new Worker(this);
    workerContext = new WorkerContext(rayConfig.workerMode, rayConfig.driverId);
  }

  /**
   * Start runtime.
   */
  public abstract void start() throws Exception;

  @Override
  public abstract void shutdown();

  @Override
  public <T> RayObject<T> put(T obj) {
    UniqueId objectId = UniqueIdUtil.computePutId(
        workerContext.getCurrentTaskId(), workerContext.nextPutIndex());

    put(objectId, obj);
    return new RayObjectImpl<>(objectId);
  }

  public <T> void put(UniqueId objectId, T obj) {
    UniqueId taskId = workerContext.getCurrentTaskId();
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
    UniqueId objectId = UniqueIdUtil.computePutId(
        workerContext.getCurrentTaskId(), workerContext.nextPutIndex());
    UniqueId taskId = workerContext.getCurrentTaskId();
    LOGGER.debug("Putting serialized object {}, for task {} ", objectId, taskId);
    objectStoreProxy.putSerialized(objectId, obj);
    return new RayObjectImpl<>(objectId);
  }

  @Override
  public <T> T get(UniqueId objectId) throws RayException {
    List<T> ret = get(ImmutableList.of(objectId));
    return ret.get(0);
  }

  @Override
  public <T> List<T> get(List<UniqueId> objectIds) {
    List<T> ret = new ArrayList<>(Collections.nCopies(objectIds.size(), null));
    boolean wasBlocked = false;

    try {
      // A map that stores the unready object ids and their original indexes.
      Map<UniqueId, Integer> unready = new HashMap<>();
      for (int i = 0; i < objectIds.size(); i++) {
        unready.put(objectIds.get(i), i);
      }
      int numAttempts = 0;

      // Repeat until we get all objects.
      while (!unready.isEmpty()) {
        List<UniqueId> unreadyIds = new ArrayList<>(unready.keySet());

        // For the initial fetch, we only fetch the objects, do not reconstruct them.
        boolean fetchOnly = numAttempts == 0;
        if (!fetchOnly) {
          // If fetchOnly is false, this worker will be blocked.
          wasBlocked = true;
        }
        // Call `fetchOrReconstruct` in batches.
        for (List<UniqueId> batch : splitIntoBatches(unreadyIds)) {
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
              UniqueId id = unreadyIds.get(i);
              ret.set(unready.get(id), getResult.object);
              unready.remove(id);
            }
          }
        }

        numAttempts += 1;
        if (LOGGER.isWarnEnabled() && numAttempts % WARN_PER_NUM_ATTEMPTS == 0) {
          // Print a warning if we've attempted too many times, but some objects are still
          // unavailable.
          List<UniqueId> idsToPrint = new ArrayList<>(unready.keySet());
          if (idsToPrint.size() > MAX_IDS_TO_PRINT_IN_WARNING) {
            idsToPrint = idsToPrint.subList(0, MAX_IDS_TO_PRINT_IN_WARNING);
          }
          String ids = idsToPrint.stream().map(UniqueId::toString)
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
  public void free(List<UniqueId> objectIds, boolean localOnly) {
    rayletClient.freePlasmaObjects(objectIds, localOnly);
  }

  private List<List<UniqueId>> splitIntoBatches(List<UniqueId> objectIds) {
    List<List<UniqueId>> batches = new ArrayList<>();
    int objectsSize = objectIds.size();

    for (int i = 0; i < objectsSize; i += FETCH_BATCH_SIZE) {
      int endIndex = i + FETCH_BATCH_SIZE;
      List<UniqueId> batchIds = (endIndex < objectsSize)
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
    TaskSpec spec = createTaskSpec(func, RayActorImpl.NIL, args, false, options);
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
      spec = createTaskSpec(func, actorImpl, args, false, null);
      spec.getExecutionDependencies().add(((RayActorImpl) actor).getTaskCursor());
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
    TaskSpec spec = createTaskSpec(actorFactoryFunc, RayActorImpl.NIL,
        args, true, options);
    RayActorImpl<?> actor = new RayActorImpl(spec.returnIds[0]);
    actor.increaseTaskCounter();
    actor.setTaskCursor(spec.returnIds[0]);
    rayletClient.submitTask(spec);
    return (RayActor<T>) actor;
  }

  /**
   * Create the task specification.
   *
   * @param func The target remote function.
   * @param actor The actor handle. If the task is not an actor task, actor id must be NIL.
   * @param args The arguments for the remote function.
   * @param isActorCreationTask Whether this task is an actor creation task.
   * @return A TaskSpec object.
   */
  private TaskSpec createTaskSpec(RayFunc func, RayActorImpl<?> actor, Object[] args,
      boolean isActorCreationTask, BaseTaskOptions taskOptions) {
    UniqueId taskId = rayletClient.generateTaskId(workerContext.getCurrentDriverId(),
        workerContext.getCurrentTaskId(), workerContext.nextTaskIndex());
    int numReturns = actor.getId().isNil() ? 1 : 2;
    UniqueId[] returnIds = UniqueIdUtil.genReturnIds(taskId, numReturns);

    UniqueId actorCreationId = UniqueId.NIL;
    if (isActorCreationTask) {
      actorCreationId = returnIds[0];
    }

    Map<String, Double> resources;
    if (null == taskOptions) {
      resources = new HashMap<>();
    } else {
      resources = new HashMap<>(taskOptions.resources);
    }

    if (!resources.containsKey(ResourceUtil.CPU_LITERAL)
        && !resources.containsKey(ResourceUtil.CPU_LITERAL.toLowerCase())) {
      resources.put(ResourceUtil.CPU_LITERAL, 0.0);
    }

    int maxActorReconstruction = 0;
    if (taskOptions instanceof ActorCreationOptions) {
      maxActorReconstruction = ((ActorCreationOptions) taskOptions).maxReconstructions;
    }

    RayFunction rayFunction = functionManager.getFunction(workerContext.getCurrentDriverId(), func);

    return new TaskSpec(
        workerContext.getCurrentDriverId(),
        taskId,
        workerContext.getCurrentTaskId(),
        -1,
        actorCreationId,
        maxActorReconstruction,
        actor.getId(),
        actor.getHandleId(),
        actor.increaseTaskCounter(),
        actor.getNewActorHandles().toArray(new UniqueId[0]),
        ArgumentsBuilder.wrap(args),
        returnIds,
        resources,
        rayFunction.getFunctionDescriptor()
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
}
