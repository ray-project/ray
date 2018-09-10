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
import org.ray.spi.impl.RedisClient;
import org.ray.spi.impl.StateStoreProxyImpl;
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
      initStateStore(params.redis_address);
    } else {
      initStateStore(params.redis_address);
      if (!isWorker) {
        List<AddressInfo> nodes = stateStoreProxy.getAddressInfo(
                            params.node_ip_address, params.redis_address, 5);
        params.object_store_name = nodes.get(0).storeName;
        params.raylet_socket_name = nodes.get(0).rayletSocketName;
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
      // initialize the links
      int releaseDelay = AbstractRayRuntime.configReader
          .getIntegerValue("ray", "plasma_default_release_delay", 0,
              "how many release requests should be delayed in plasma client");

      ObjectStoreLink plink = new PlasmaClient(params.object_store_name, "", releaseDelay);
      LocalSchedulerLink slink = new DefaultLocalSchedulerClient(
              params.raylet_socket_name,
              WorkerContext.currentWorkerId(),
              isWorker,
              WorkerContext.currentTask().taskId
      );

      init(slink, plink, funcMgr, pathConfig);

      // register
      registerWorker(isWorker, params.node_ip_address, params.object_store_name,
              params.raylet_socket_name);

    }

    RayLog.core.info("RayNativeRuntime started with store {}, raylet {}",
        params.object_store_name, params.raylet_socket_name);
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
    params.raylet_socket_name = manager.info().localStores.get(0).rayletSocketName;
    //params.node_ip_address = NetworkUtil.getIpAddress();
  }

  private void initStateStore(String redisAddress) throws Exception {
    kvStore = new RedisClient();
    kvStore.setAddr(redisAddress);
    stateStoreProxy = new StateStoreProxyImpl(kvStore);
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

}
