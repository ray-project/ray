package org.ray.core.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.plasma.PlasmaClient;
import org.ray.core.AbstractRayRuntime;
import org.ray.core.WorkerContext;
import org.ray.core.model.RayParameters;
import org.ray.core.model.WorkerMode;
import org.ray.runner.RunManager;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.LocalSchedulerLink;
import org.ray.spi.NopRemoteFunctionManager;
import org.ray.spi.PathConfig;
import org.ray.spi.RemoteFunctionManager;
import org.ray.spi.StateStoreProxy;
import org.ray.spi.impl.DefaultLocalSchedulerClient;
import org.ray.spi.impl.NativeRemoteFunctionManager;
import org.ray.spi.impl.NonRayletStateStoreProxyImpl;
import org.ray.spi.impl.RayletStateStoreProxyImpl;
import org.ray.spi.impl.RedisClient;
import org.ray.spi.model.AddressInfo;
import org.ray.util.logger.RayLog;

/**
 * native runtime for local box and cluster run.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  static {
    System.err.println("Current working directory is " + System.getProperty("user.dir"));
    System.loadLibrary("local_scheduler_library_java");
    System.loadLibrary("plasma_java");
  }

  private StateStoreProxy stateStoreProxy;
  private KeyValueStoreLink kvStore = null;
  private RunManager manager = null;

  protected RayNativeRuntime() {
  }

  @Override
  public void start(RayParameters params) throws Exception {
    boolean isWorker = (params.worker_mode == WorkerMode.WORKER);
    PathConfig pathConfig = new PathConfig(configReader);

    // initialize params
    if (params.redis_address.length() == 0) {
      if (isWorker) {
        throw new Error("Redis address must be configured under Worker mode.");
      }
      startOnebox(params, pathConfig);
      initStateStore(params.redis_address, params.use_raylet);
    } else {
      initStateStore(params.redis_address, params.use_raylet);
      if (!isWorker) {
        List<AddressInfo> nodes = stateStoreProxy.getAddressInfo(
                            params.node_ip_address, params.redis_address, 5);
        params.object_store_name = nodes.get(0).storeName;
        if (!params.use_raylet) {
          params.object_store_manager_name = nodes.get(0).managerName;
          params.local_scheduler_name = nodes.get(0).schedulerName;
        } else {
          params.raylet_socket_name = nodes.get(0).rayletSocketName;
        }
      }
    }

    // initialize remote function manager
    RemoteFunctionManager funcMgr = params.run_mode.isDevPathManager()
        ? new NopRemoteFunctionManager(params.driver_id) : new NativeRemoteFunctionManager(kvStore);

    // initialize worker context
    if (params.worker_mode == WorkerMode.DRIVER) {
      // TODO: The relationship between workerID, driver_id and dummy_task.driver_id should be
      // recheck carefully
      WorkerContext.workerID = params.driver_id;
    }
    WorkerContext.init(params);

    if (params.onebox_delay_seconds_before_run_app_logic > 0) {
      for (int i = 0; i < params.onebox_delay_seconds_before_run_app_logic; ++i) {
        System.err.println("Pause for debugger, "
            + (params.onebox_delay_seconds_before_run_app_logic - i)
            + " seconds left ...");
        Thread.sleep(1000);
      }
    }

    if (params.worker_mode != WorkerMode.NONE) {
      String overwrites = "";
      // initialize the links
      int releaseDelay = AbstractRayRuntime.configReader
          .getIntegerValue("ray", "plasma_default_release_delay", 0,
              "how many release requests should be delayed in plasma client");

      if (!params.use_raylet) {
        ObjectStoreLink plink = new PlasmaClient(params.object_store_name,
            params.object_store_manager_name, releaseDelay);

        LocalSchedulerLink slink = new DefaultLocalSchedulerClient(
            params.local_scheduler_name,
            WorkerContext.currentWorkerId(),
            isWorker,
            WorkerContext.currentTask().taskId,
            false
        );

        init(slink, plink, funcMgr, pathConfig);

        // register
        registerWorker(isWorker, params.node_ip_address, params.object_store_name,
            params.object_store_manager_name, params.local_scheduler_name);
      } else {

        ObjectStoreLink plink = new PlasmaClient(params.object_store_name, "", releaseDelay);

        LocalSchedulerLink slink = new DefaultLocalSchedulerClient(
            params.raylet_socket_name,
            WorkerContext.currentWorkerId(),
            isWorker,
            WorkerContext.currentTask().taskId,
            true
        );

        init(slink, plink, funcMgr, pathConfig);

        // register
        registerWorker(isWorker, params.node_ip_address, params.object_store_name,
            params.raylet_socket_name);
      }
    }

    RayLog.core.info("RayNativeRuntime start with "
        + "store " + params.object_store_name
        + ", manager " + params.object_store_manager_name
        + ", scheduler " + params.local_scheduler_name
    );
  }

  @Override
  public void shutdown() {
    if (null != manager) {
      manager.cleanup(true);
    }
  }

  private void startOnebox(RayParameters params, PathConfig paths) throws Exception {
    params.cleanup = true;
    manager = new RunManager(params, paths, AbstractRayRuntime.configReader);
    manager.startRayHead();

    params.redis_address = manager.info().redisAddress;
    params.object_store_name = manager.info().localStores.get(0).storeName;
    params.object_store_manager_name = manager.info().localStores.get(0).managerName;
    params.local_scheduler_name = manager.info().localStores.get(0).schedulerName;
    params.raylet_socket_name = manager.info().localStores.get(0).rayletSocketName;
    //params.node_ip_address = NetworkUtil.getIpAddress();
  }

  private void initStateStore(String redisAddress, boolean useRaylet) throws Exception {
    kvStore = new RedisClient();
    kvStore.setAddr(redisAddress);
    stateStoreProxy = useRaylet
            ? new RayletStateStoreProxyImpl(kvStore)
            : new NonRayletStateStoreProxyImpl(kvStore);
    //stateStoreProxy.setStore(kvStore);
    stateStoreProxy.initializeGlobalState();
  }

  private void registerWorker(boolean isWorker, String nodeIpAddress, String storeName,
                              String rayletSocketName) {
    Map<String, String> workerInfo = new HashMap<>();
    String workerId = new String(WorkerContext.currentWorkerId().getBytes());
    if (!isWorker) {
      workerInfo.put("node_ip_address", nodeIpAddress);
      workerInfo.put("driver_id", workerId);
      workerInfo.put("start_time", String.valueOf(System.currentTimeMillis()));
      workerInfo.put("plasma_store_socket", storeName);
      workerInfo.put("raylet_socket", rayletSocketName);
      workerInfo.put("name", System.getProperty("user.dir"));
      //TODO: worker.redis_client.hmset(b"Drivers:" + worker.workerId, driver_info)
      kvStore.hmset("Drivers:" + workerId, workerInfo);
    } else {
      workerInfo.put("node_ip_address", nodeIpAddress);
      workerInfo.put("plasma_store_socket", storeName);
      workerInfo.put("raylet_socket", rayletSocketName);
      //TODO: b"Workers:" + worker.workerId,
      kvStore.hmset("Workers:" + workerId, workerInfo);
    }
  }

  private void registerWorker(boolean isWorker, String nodeIpAddress, String storeName,
                              String managerName, String schedulerName) {
    Map<String, String> workerInfo = new HashMap<>();
    String workerId = new String(WorkerContext.currentWorkerId().getBytes());
    if (!isWorker) {
      workerInfo.put("node_ip_address", nodeIpAddress);
      workerInfo.put("driver_id", workerId);
      workerInfo.put("start_time", String.valueOf(System.currentTimeMillis()));
      workerInfo.put("plasma_store_socket", storeName);
      workerInfo.put("plasma_manager_socket", managerName);
      workerInfo.put("local_scheduler_socket", schedulerName);
      workerInfo.put("name", System.getProperty("user.dir"));
      //TODO: worker.redis_client.hmset(b"Drivers:" + worker.workerId, driver_info)
      kvStore.hmset("Drivers:" + workerId, workerInfo);
    } else {
      workerInfo.put("node_ip_address", nodeIpAddress);
      workerInfo.put("plasma_store_socket", storeName);
      workerInfo.put("plasma_manager_socket", managerName);
      workerInfo.put("local_scheduler_socket", schedulerName);
      //TODO: b"Workers:" + worker.workerId,
      kvStore.hmset("Workers:" + workerId, workerInfo);
    }
  }
}
