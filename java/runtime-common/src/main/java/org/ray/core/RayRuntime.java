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
import org.ray.api.RayApi;
import org.ray.api.RayObject;
import org.ray.api.RayObjects;
import org.ray.api.UniqueID;
import org.ray.api.WaitResult;
import org.ray.api.funcs.RayFunc;
import org.ray.core.model.RayParameters;
import org.ray.spi.LocalSchedulerLink;
import org.ray.spi.LocalSchedulerProxy;
import org.ray.spi.ObjectStoreProxy;
import org.ray.spi.ObjectStoreProxy.GetStatus;
import org.ray.spi.PathConfig;
import org.ray.spi.RemoteFunctionManager;
import org.ray.util.config.ConfigReader;
import org.ray.util.exception.TaskExecutionException;
import org.ray.util.logger.RayLog;

/**
 * Core functionality to implement Ray APIs.
 */
public abstract class RayRuntime implements RayApi {

  public static ConfigReader configReader;
  protected static RayRuntime ins = null;
  protected static RayParameters params = null;
  private static boolean fromRayInit = false;
  protected Worker worker;
  protected LocalSchedulerProxy localSchedulerProxy;
  protected ObjectStoreProxy objectStoreProxy;
  protected LocalFunctionManager functions;
  protected RemoteFunctionManager remoteFunctionManager;
  protected PathConfig pathConfig;

  // app level Ray.init()
  // make it private so there is no direct usage but only from Ray.init
  private static RayRuntime init() {
    if (ins == null) {
      try {
        fromRayInit = true;
        RayRuntime.init(null, null);
        fromRayInit = false;
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Ray.init failed", e);
      }
    }
    return ins;
  }

  // engine level RayRuntime.init(xx, xx)
  // updateConfigStr is sth like section1.k1=v1;section2.k2=v2
  public static RayRuntime init(String configPath, String updateConfigStr) throws Exception {
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
      RayRuntime.params = new RayParameters(configReader);

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
  public static RayRuntime init(String[] args) throws Exception {
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
    UniqueIdHelper.setThreadRandomSeed(UniqueIdHelper.getUniqueness(params.driver_id));
    remoteFunctionManager = remoteLoader;
    pathConfig = pathManager;

    functions = new LocalFunctionManager(remoteLoader);
    localSchedulerProxy = new LocalSchedulerProxy(slink);

    if (!params.use_raylet) {
      objectStoreProxy = new ObjectStoreProxy(plink);
    } else {
      objectStoreProxy = new ObjectStoreProxy(plink, slink);
    }
    
    worker = new Worker(localSchedulerProxy, functions);
  }

  private static RayRuntime instantiate(RayParameters params) {
    String className = params.run_mode.isNativeRuntime()
        ? "org.ray.core.impl.RayNativeRuntime" : "org.ray.core.impl.RayDevRuntime";

    RayRuntime runtime;
    try {
      Class<?> cls = Class.forName(className);
      if (cls.getConstructors().length > 0) {
        throw new Error("The RayRuntime final class should not have any public constructor.");
      }
      Constructor<?> cons = cls.getDeclaredConstructor();
      cons.setAccessible(true);
      runtime = (RayRuntime) cons.newInstance();
      cons.setAccessible(false);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | SecurityException | ClassNotFoundException
        | NoSuchMethodException e) {
      RayLog.core
          .error("Load class " + className + " failed for run-mode " + params.run_mode.toString(),
              e);
      throw new Error("RayRuntime not registered for run-mode " + params.run_mode.toString());
    }

    RayLog.core
        .info("Start " + runtime.getClass().getName() + " with " + params.run_mode.toString());
    try {
      runtime.start(params);
    } catch (Exception e) {
      System.err.println("RayRuntime start failed:" + e.getMessage()); //in case of logger not ready
      e.printStackTrace(); //in case of logger not ready
      RayLog.core.error("RayRuntime start failed", e);
      System.exit(-1);
    }

    return runtime;
  }

  /**
   * start runtime.
   */
  public abstract void start(RayParameters params) throws Exception;

  public static RayRuntime getInstance() {
    return ins;
  }

  public static RayParameters getParams() {
    return params;
  }

  public abstract void cleanUp();

  public <T> void putRaw(UniqueID taskId, UniqueID objectId, T obj) {
    putRaw(taskId, objectId, obj, null);
  }

  /***********
   * RayApi methods.
   ***********/

  public <T, TMT> void putRaw(UniqueID taskId, UniqueID objectId, T obj, TMT metadata) {
    RayLog.core.info("Task " + taskId.toString() + " Object " + objectId.toString() + " put");
    if (!params.use_raylet) {
      localSchedulerProxy.markTaskPutDependency(taskId, objectId);
    }
    objectStoreProxy.put(objectId, obj, metadata);
  }

  public <T> void putRaw(UniqueID objectId, T obj) {
    UniqueID taskId = getCurrentTaskId();
    putRaw(taskId, objectId, obj, null);
  }

  public <T> void putRaw(T obj) {
    UniqueID taskId = getCurrentTaskId();
    UniqueID objectId = getCurrentTaskNextPutId();
    putRaw(taskId, objectId, obj, null);
  }

  /**
   * get the task identity of the currently running task, UniqueID.Nil if not inside any
   */
  public UniqueID getCurrentTaskId() {
    return worker.getCurrentTaskId();
  }

  /**
   * get the to-be-returned objects identities of the currently running task, empty array if not
   * inside any.
   */
  public UniqueID getCurrentTaskNextPutId() {
    return worker.getCurrentTaskNextPutId();
  }

  @Override
  public <T> RayObject<T> put(T obj) {
    return put(obj, null);
  }

  @Override
  public <T, TMT> RayObject<T> put(T obj, TMT metadata) {
    UniqueID taskId = getCurrentTaskId();
    UniqueID objectId = getCurrentTaskNextPutId();
    putRaw(taskId, objectId, obj, metadata);
    return new RayObject<>(objectId);
  }

  @Override
  public <T> T get(UniqueID objectId) throws TaskExecutionException {
    return doGet(objectId, false);
  }

  @Override
  public <T> List<T> get(List<UniqueID> objectIds) throws TaskExecutionException {
    return doGet(objectIds, false);
  }

  @Override
  public <T> T getMeta(UniqueID objectId) throws TaskExecutionException {
    return doGet(objectId, true);
  }

  @Override
  public <T> List<T> getMeta(List<UniqueID> objectIds) throws TaskExecutionException {
    return doGet(objectIds, true);
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitfor, int numReturns, int timeout) {
    return objectStoreProxy.wait(waitfor, numReturns, timeout);
  }

  @Override
  public RayObjects call(UniqueID taskId, Class<?> funcCls, RayFunc lambda, int returnCount,
      Object... args) {
    return worker.rpc(taskId, funcCls, lambda, returnCount, args);
  }

  private <T> List<T> doGet(List<UniqueID> objectIds, boolean isMetadata)
      throws TaskExecutionException {
    boolean wasBlocked = false;
    UniqueID taskId = getCurrentTaskId();

    try {
      int numObjectIds = objectIds.size();

      // Do an initial fetch for remote objects.
      List<List<UniqueID>> fetchBatches =
          splitIntoBatches(objectIds, params.worker_fetch_request_size);
      for (List<UniqueID> batch : fetchBatches) {
        if (!params.use_raylet) {
          objectStoreProxy.fetch(batch);
        } else {
          localSchedulerProxy.reconstructObjects(batch, true);
        }
      }

      // Get the objects. We initially try to get the objects immediately.
      List<Pair<T, GetStatus>> ret = objectStoreProxy
          .get(objectIds, params.default_first_check_timeout_ms, isMetadata);
      assert ret.size() == numObjectIds;

      // Mapping the object IDs that we haven't gotten yet to their original index in objectIds.
      Map<UniqueID, Integer> unreadys = new HashMap<>();
      for (int i = 0; i < numObjectIds; i++) {
        if (ret.get(i).getRight() != GetStatus.SUCCESS) {
          unreadys.put(objectIds.get(i), i);
        }
      }
      wasBlocked = (unreadys.size() > 0);

      // Try reconstructing any objects we haven't gotten yet. Try to get them
      // until at least PlasmaLink.GET_TIMEOUT_MS milliseconds passes, then repeat.
      while (unreadys.size() > 0) {
        List<UniqueID> unreadyList = new ArrayList<>(unreadys.keySet());
        List<List<UniqueID>> reconstructBatches =
            splitIntoBatches(unreadyList, params.worker_fetch_request_size);

        for (List<UniqueID> batch : reconstructBatches) {
          if (!params.use_raylet) {
            for (UniqueID objectId : batch) {
              localSchedulerProxy.reconstructObject(objectId, false);
            }
            // Do another fetch for objects that aren't available locally yet, in case
            // they were evicted since the last fetch.
            objectStoreProxy.fetch(batch);
          } else {
            localSchedulerProxy.reconstructObjects(batch, false);
          }
        }

        List<Pair<T, GetStatus>> results = objectStoreProxy
            .get(unreadyList, params.default_get_check_interval_ms, isMetadata);

        // Remove any entries for objects we received during this iteration so we
        // don't retrieve the same object twice.
        for (int i = 0; i < results.size(); i++) {
          Pair<T, GetStatus> value = results.get(i);
          if (value.getRight() == GetStatus.SUCCESS) {
            UniqueID id = unreadyList.get(i);
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
        localSchedulerProxy.notifyUnblocked();
      }
    }
  }

  private <T> T doGet(UniqueID objectId, boolean isMetadata) throws TaskExecutionException {
    ImmutableList<UniqueID> objectIds = ImmutableList.of(objectId);
    List<T> results = doGet(objectIds, isMetadata);

    assert results.size() == 1;
    return results.get(0);
  }

  private List<List<UniqueID>> splitIntoBatches(List<UniqueID> objectIds, int batchSize) {
    List<List<UniqueID>> batches = new ArrayList<>();
    int objectsSize = objectIds.size();

    for (int i = 0; i < objectsSize; i += batchSize) {
      int endIndex = i + batchSize;
      List<UniqueID> batchIds = (endIndex < objectsSize)
          ? objectIds.subList(i, endIndex)
          : objectIds.subList(i, objectsSize);

      batches.add(batchIds);
    }

    return batches;
  }

  /**
   * get the object put identity of the currently running task, UniqueID.Nil if not inside any
   */
  public UniqueID[] getCurrentTaskReturnIDs() {
    return worker.getCurrentTaskReturnIDs();
  }

  /***********
   * Internal Methods.
   ***********/

  public void loop() {
    worker.loop();
  }

  /**
   * get actor with given id.
   */
  public abstract Object getLocalActor(UniqueID id);

  public PathConfig getPaths() {
    return pathConfig;
  }

  public RemoteFunctionManager getRemoteFunctionManager() {
    return remoteFunctionManager;
  }
}