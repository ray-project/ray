package org.ray.core;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.function.RayFunc;
import org.ray.api.id.UniqueId;
import org.ray.api.runtime.RayRuntime;
import org.ray.core.model.RayParameters;
import org.ray.spi.LocalSchedulerLink;
import org.ray.spi.ObjectStoreProxy;
import org.ray.spi.ObjectStoreProxy.GetStatus;
import org.ray.spi.PathConfig;
import org.ray.spi.RemoteFunctionManager;
import org.ray.spi.model.RayMethod;
import org.ray.spi.model.TaskSpec;
import org.ray.util.MethodId;
import org.ray.util.ResourceUtil;
import org.ray.util.config.ConfigReader;
import org.ray.util.exception.TaskExecutionException;
import org.ray.util.logger.RayLog;

/**
 * Core functionality to implement Ray APIs.
 */
public abstract class AbstractRayRuntime implements RayRuntime {

  public static ConfigReader configReader;
  protected static AbstractRayRuntime ins = null;
  protected static RayParameters params = null;
  private static boolean fromRayInit = false;
  protected Worker worker;
  protected LocalSchedulerLink localSchedulerClient;
  protected ObjectStoreProxy objectStoreProxy;
  protected LocalFunctionManager functions;
  protected RemoteFunctionManager remoteFunctionManager;
  protected PathConfig pathConfig;

  /**
   * Actor ID -> local actor instance.
   */
  Map<UniqueId, Object> localActors = new HashMap<>();

  // app level Ray.init()
  // make it private so there is no direct usage but only from Ray.init
  private static AbstractRayRuntime init() {
    if (ins == null) {
      try {
        fromRayInit = true;
        AbstractRayRuntime.init(null, null);
        fromRayInit = false;
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Ray.init failed", e);
      }
    }
    return ins;
  }

  // engine level AbstractRayRuntime.init(xx, xx)
  // updateConfigStr is sth like section1.k1=v1;section2.k2=v2
  public static AbstractRayRuntime init(String configPath, String updateConfigStr)
      throws Exception {
    if (ins == null) {
      if (configPath == null) {
        configPath = System.getenv("RAY_CONFIG");
        if (configPath == null) {
          configPath = System.getProperty("ray.config");
        }
        if (configPath == null) {
          throw new Exception(
              "Please set config file path in env RAY_CONFIG or property ray.config");
        }
      }
      configReader = new ConfigReader(configPath, updateConfigStr);
      AbstractRayRuntime.params = new RayParameters(configReader);

      RayLog.init(params.log_dir);
      assert RayLog.core != null;

      ins = instantiate(params);
      assert (ins != null);

      if (!fromRayInit) {
        Ray.init(); // assign Ray._impl
      }
    }
    return ins;
  }

  // init with command line args
  // --config=ray.config.ini --overwrite=updateConfigStr
  public static AbstractRayRuntime init(String[] args) throws Exception {
    String config = null;
    String updateConfig = null;
    for (String arg : args) {
      if (arg.startsWith("--config=")) {
        config = arg.substring("--config=".length());
      } else if (arg.startsWith("--overwrite=")) {
        updateConfig = arg.substring("--overwrite=".length());
      } else {
        throw new RuntimeException("Input argument " + arg
            + " is not recognized, please use --overwrite to merge it into config file");
      }
    }
    return init(config, updateConfig);
  }

  protected void init(
      LocalSchedulerLink slink,
      ObjectStoreLink plink,
      RemoteFunctionManager remoteLoader,
      PathConfig pathManager
  ) {
    remoteFunctionManager = remoteLoader;
    pathConfig = pathManager;

    functions = new LocalFunctionManager(remoteLoader);
    localSchedulerClient = slink;

    objectStoreProxy = new ObjectStoreProxy(plink);
    worker = new Worker(this);
  }

  private static AbstractRayRuntime instantiate(RayParameters params) {
    String className = params.run_mode.isNativeRuntime()
        ? "org.ray.core.impl.RayNativeRuntime" : "org.ray.core.impl.RayDevRuntime";

    AbstractRayRuntime runtime;
    try {
      Class<?> cls = Class.forName(className);
      if (cls.getConstructors().length > 0) {
        throw new Error(
            "The AbstractRayRuntime final class should not have any public constructor.");
      }
      Constructor<?> cons = cls.getDeclaredConstructor();
      cons.setAccessible(true);
      runtime = (AbstractRayRuntime) cons.newInstance();
      cons.setAccessible(false);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | SecurityException | ClassNotFoundException
        | NoSuchMethodException e) {
      RayLog.core
          .error("Load class " + className + " failed for run-mode " + params.run_mode.toString(),
              e);
      throw new Error("AbstractRayRuntime not registered for run-mode "
          + params.run_mode.toString());
    }

    RayLog.core
        .info("Start " + runtime.getClass().getName() + " with " + params.run_mode.toString());
    try {
      runtime.start(params);
    } catch (Exception e) {
      RayLog.core.error("Failed to init RayRuntime", e);
      System.exit(-1);
    }

    return runtime;
  }

  /**
   * start runtime.
   */
  public abstract void start(RayParameters params) throws Exception;

  public static AbstractRayRuntime getInstance() {
    return ins;
  }

  public static RayParameters getParams() {
    return params;
  }

  @Override
  public abstract void shutdown();

  @Override
  public <T> RayObject<T> put(T obj) {
    UniqueId objectId = UniqueIdHelper.computePutId(
        WorkerContext.currentTask().taskId, WorkerContext.nextPutIndex());

    put(objectId, obj);
    return new RayObjectImpl<>(objectId);
  }

  public <T> void put(UniqueId objectId, T obj) {
    UniqueId taskId = WorkerContext.currentTask().taskId;
    RayLog.core.info("Putting object {}, for task {} ", objectId, taskId);
    objectStoreProxy.put(objectId, obj, null);
  }

  @Override
  public <T> T get(UniqueId objectId) throws TaskExecutionException {
    List<T> ret = get(ImmutableList.of(objectId));
    return ret.get(0);
  }

  @Override
  public <T> List<T> get(List<UniqueId> objectIds) {
    boolean wasBlocked = false;
    UniqueId taskId = WorkerContext.currentTask().taskId;

    try {
      int numObjectIds = objectIds.size();

      // Do an initial fetch for remote objects.
      List<List<UniqueId>> fetchBatches =
          splitIntoBatches(objectIds, params.worker_fetch_request_size);
      for (List<UniqueId> batch : fetchBatches) {
        localSchedulerClient.reconstructObjects(batch, true);
      }

      // Get the objects. We initially try to get the objects immediately.
      List<Pair<T, GetStatus>> ret = objectStoreProxy
          .get(objectIds, params.default_first_check_timeout_ms, false);
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
      while (unreadys.size() > 0) {
        List<UniqueId> unreadyList = new ArrayList<>(unreadys.keySet());
        List<List<UniqueId>> reconstructBatches =
            splitIntoBatches(unreadyList, params.worker_fetch_request_size);

        for (List<UniqueId> batch : reconstructBatches) {
          localSchedulerClient.reconstructObjects(batch, false);
        }

        List<Pair<T, GetStatus>> results = objectStoreProxy
            .get(unreadyList, params.default_get_check_interval_ms, false);

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
      }

      RayLog.core
          .debug("Task " + taskId + " Objects " + Arrays.toString(objectIds.toArray()) + " get");
      List<T> finalRet = new ArrayList<>();

      for (Pair<T, GetStatus> value : ret) {
        finalRet.add(value.getLeft());
      }

      return finalRet;
    } catch (TaskExecutionException e) {
      RayLog.core.error("Task " + taskId + " Objects " + Arrays.toString(objectIds.toArray())
          + " get with Exception", e);
      throw e;
    } finally {
      // If there were objects that we weren't able to get locally, let the local
      // scheduler know that we're now unblocked.
      if (wasBlocked) {
        localSchedulerClient.notifyUnblocked();
      }
    }
  }

  @Override
  public void free(List<UniqueId> objectIds, boolean localOnly) {
    localSchedulerClient.freePlasmaObjects(objectIds, localOnly);
  }

  private List<List<UniqueId>> splitIntoBatches(List<UniqueId> objectIds, int batchSize) {
    List<List<UniqueId>> batches = new ArrayList<>();
    int objectsSize = objectIds.size();

    for (int i = 0; i < objectsSize; i += batchSize) {
      int endIndex = i + batchSize;
      List<UniqueId> batchIds = (endIndex < objectsSize)
          ? objectIds.subList(i, endIndex)
          : objectIds.subList(i, objectsSize);

      batches.add(batchIds);
    }

    return batches;
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs) {
    return localSchedulerClient.wait(waitList, numReturns, timeoutMs);
  }

  @Override
  public RayObject call(RayFunc func, Object[] args) {
    TaskSpec spec = createTaskSpec(func, RayActorImpl.NIL, args, false);
    localSchedulerClient.submitTask(spec);
    return new RayObjectImpl(spec.returnIds[0]);
  }

  @Override
  public RayObject call(RayFunc func, RayActor actor, Object[] args) {
    if (!(actor instanceof RayActorImpl)) {
      throw new IllegalArgumentException("Unsupported actor type: " + actor.getClass().getName());
    }
    RayActorImpl actorImpl = (RayActorImpl)actor;
    TaskSpec spec = createTaskSpec(func, actorImpl, args, false);
    actorImpl.setTaskCursor(spec.returnIds[1]);
    localSchedulerClient.submitTask(spec);
    return new RayObjectImpl(spec.returnIds[0]);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> RayActor<T> createActor(RayFunc actorFactoryFunc, Object[] args) {
    TaskSpec spec = createTaskSpec(actorFactoryFunc, RayActorImpl.NIL, args, true);
    RayActorImpl<?> actor = new RayActorImpl(spec.returnIds[0]);
    actor.increaseTaskCounter();
    actor.setTaskCursor(spec.returnIds[0]);
    localSchedulerClient.submitTask(spec);
    return (RayActor<T>) actor;
  }

  /**
   * Generate the return ids of a task.
   */
  private UniqueId[] genReturnIds(UniqueId taskId, int numReturns) {
    UniqueId[] ret = new UniqueId[numReturns];
    for (int i = 0; i < numReturns; i++) {
      ret[i] = UniqueIdHelper.computeReturnId(taskId, i + 1);
    }
    return ret;
  }

  /**
   * Create the task specification.
   * @param func The target remote function.
   * @param actor The actor handle. If the task is not an actor task, actor id must be NIL.
   * @param args The arguments for the remote function.
   * @param isActorCreationTask Whether this task is an actor creation task.
   * @return A TaskSpec object.
   */
  private TaskSpec createTaskSpec(RayFunc func, RayActorImpl actor, Object[] args,
      boolean isActorCreationTask) {
    final TaskSpec current = WorkerContext.currentTask();
    UniqueId taskId = localSchedulerClient.generateTaskId(current.driverId,
        current.taskId,
        WorkerContext.nextCallIndex());
    int numReturns = actor.getId().isNil() ? 1 : 2;
    UniqueId[] returnIds = genReturnIds(taskId, numReturns);

    UniqueId actorCreationId = UniqueId.NIL;
    if (isActorCreationTask) {
      actorCreationId = returnIds[0];
    }

    MethodId methodId = MethodId.fromSerializedLambda(func);

    // NOTE: we append the class name at the end of arguments,
    // so that we can look up the method based on the class name.
    // TODO(hchen): move class name to task spec.
    args = Arrays.copyOf(args, args.length + 1);
    args[args.length - 1] = methodId.className;

    RayMethod rayMethod = functions.getMethod(
        current.driverId, actor.getId(), new UniqueId(methodId.getSha1Hash()), methodId.className
    ).getRight();
    UniqueId funcId = rayMethod.getFuncId();

    return new TaskSpec(
        current.driverId,
        taskId,
        current.taskId,
        -1,
        actor.getId(),
        actor.increaseTaskCounter(),
        funcId,
        ArgumentsBuilder.wrap(args),
        returnIds,
        actor.getHandleId(),
        actorCreationId,
        ResourceUtil.getResourcesMapFromArray(rayMethod.remoteAnnotation),
        actor.getTaskCursor()
    );
  }

  public void loop() {
    worker.loop();
  }

  public Worker getWorker() {
    return worker;
  }

  public LocalSchedulerLink getLocalSchedulerClient() {
    return localSchedulerClient;
  }

  public LocalFunctionManager getLocalFunctionManager() {
    return functions;
  }
}

