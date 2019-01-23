package org.ray.runtime;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
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
import org.ray.runtime.objectstore.ObjectStoreProxy.GetStatus;
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

  private static final int GET_TIMEOUT_MS = 1000;
  private static final int FETCH_BATCH_SIZE = 1000;
  private static final int LIMITED_RETRY_COUNTER = 10;

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
    objectStoreProxy.put(objectId, obj, null);
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
    objectStoreProxy.putSerialized(objectId, obj, null);
    return new RayObjectImpl<>(objectId);
  }

  @Override
  public <T> T get(UniqueId objectId) throws RayException {
    List<T> ret = get(ImmutableList.of(objectId));
    return ret.get(0);
  }

  @Override
  public <T> List<T> get(List<UniqueId> objectIds) {
    boolean wasBlocked = false;

    try {
      int numObjectIds = objectIds.size();

      // Do an initial fetch for remote objects.
      List<List<UniqueId>> fetchBatches = splitIntoBatches(objectIds);
      for (List<UniqueId> batch : fetchBatches) {
        rayletClient.fetchOrReconstruct(batch, true, workerContext.getCurrentTaskId());
      }

      // Get the objects. We initially try to get the objects immediately.
      List<Pair<T, GetStatus>> ret = objectStoreProxy
          .get(objectIds, GET_TIMEOUT_MS, false);
      assert ret.size() == numObjectIds;

      // Mapping the object IDs that we haven't gotten yet to their original index in objectIds.
      Map<UniqueId, Integer> unreadys = new HashMap<>();
      for (int i = 0; i < numObjectIds; i++) {
        if (ret.get(i).getRight() != GetStatus.SUCCESS) {
          unreadys.put(objectIds.get(i), i);
        }
      }
      wasBlocked = (unreadys.size() > 0);

      // Try reconstructing any objects we haven't gotten yet. Try to get them
      // until at least PlasmaLink.GET_TIMEOUT_MS milliseconds passes, then repeat.
      int retryCounter = 0;
      while (unreadys.size() > 0) {
        retryCounter++;
        List<UniqueId> unreadyList = new ArrayList<>(unreadys.keySet());
        List<List<UniqueId>> reconstructBatches = splitIntoBatches(unreadyList);

        for (List<UniqueId> batch : reconstructBatches) {
          rayletClient.fetchOrReconstruct(batch, false, workerContext.getCurrentTaskId());
        }

        List<Pair<T, GetStatus>> results = objectStoreProxy
            .get(unreadyList, GET_TIMEOUT_MS, false);

        // Remove any entries for objects we received during this iteration so we
        // don't retrieve the same object twice.
        for (int i = 0; i < results.size(); i++) {
          Pair<T, GetStatus> value = results.get(i);
          if (value.getRight() == GetStatus.SUCCESS) {
            UniqueId id = unreadyList.get(i);
            ret.set(unreadys.get(id), value);
            unreadys.remove(id);
          }
        }

        if (retryCounter % LIMITED_RETRY_COUNTER == 0) {
          LOGGER.warn("Attempted {} times to reconstruct objects {}, "
              + "but haven't received response. If this message continues to print,"
              + " it may indicate that the task is hanging, or someting wrong "
              + "happened in raylet backend.",
              retryCounter, unreadys.keySet());
        }
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Got objects {} for task {}.", Arrays.toString(objectIds.toArray()),
            workerContext.getCurrentTaskId());
      }

      List<T> finalRet = new ArrayList<>();

      for (Pair<T, GetStatus> value : ret) {
        finalRet.add(value.getLeft());
      }

      return finalRet;
    } catch (RayException e) {
      LOGGER.error("Failed to get objects for task {}.", workerContext.getCurrentTaskId(), e);
      throw e;
    } finally {
      // If there were objects that we weren't able to get locally, let the local
      // scheduler know that we're now unblocked.
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

  public FunctionManager getFunctionManager() {
    return functionManager;
  }

  public RayConfig getRayConfig() {
    return rayConfig;
  }
}
