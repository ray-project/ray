package org.ray.runtime;

import java.util.HashMap;
import java.util.Map;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.runner.RunManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native runtime for cluster mode.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  private RunManager manager = null;

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  @Override
  public void start() {
    super.start();

    if (rayConfig.get().getRedisAddress() == null) {
      manager = new RunManager(rayConfig.get());
      manager.startRayProcesses(true);
    }

    gcsClient = new GcsClient(rayConfig.get().getRedisAddress(), rayConfig.get().redisPassword);

    worker.set(new Worker(rayConfig.get().workerMode, this, functionManager,
        rayConfig.get().objectStoreSocketName, rayConfig.get().rayletSocketName,
        rayConfig.get().workerMode == WorkerMode.DRIVER ? rayConfig.get().jobId : UniqueId.NIL));

    // register
    registerWorker();

    LOGGER.info("RayNativeRuntime started with store {}, raylet {}",
        rayConfig.get().objectStoreSocketName, rayConfig.get().rayletSocketName);
  }

  @Override
  public void shutdown() {
    if (null != manager) {
      manager.cleanup();
    }
  }

  /**
   * Register this worker or driver to GCS.
   */
  private void registerWorker() {
    RedisClient redisClient = new RedisClient(rayConfig.get().getRedisAddress(), rayConfig.get().redisPassword);
    Map<String, String> workerInfo = new HashMap<>();
    String workerId = new String(worker.get().getWorkerContext().getCurrentWorkerId().getBytes());
    if (rayConfig.get().workerMode == WorkerMode.DRIVER) {
      workerInfo.put("node_ip_address", rayConfig.get().nodeIp);
      workerInfo.put("driver_id", workerId);
      workerInfo.put("start_time", String.valueOf(System.currentTimeMillis()));
      workerInfo.put("plasma_store_socket", rayConfig.get().objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.get().rayletSocketName);
      workerInfo.put("name", System.getProperty("user.dir"));
      //TODO: worker.redis_client.hmset(b"Drivers:" + worker.workerId, driver_info)
      redisClient.hmset("Drivers:" + workerId, workerInfo);
    } else {
      workerInfo.put("node_ip_address", rayConfig.get().nodeIp);
      workerInfo.put("plasma_store_socket", rayConfig.get().objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.get().rayletSocketName);
      //TODO: b"Workers:" + worker.workerId,
      redisClient.hmset("Workers:" + workerId, workerInfo);
    }
  }
}
