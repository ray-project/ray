package org.ray.runtime;

import com.google.common.base.Strings;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.plasma.PlasmaClient;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.WorkerMode;
import org.ray.runtime.gcs.KeyValueStoreLink;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.RayletClientImpl;
import org.ray.runtime.runner.RunManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * native runtime for local box and cluster run.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  private KeyValueStoreLink kvStore = null;
  private RunManager manager = null;

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private void resetLibraryPath() {
    String path = System.getProperty("java.library.path");
    if (Strings.isNullOrEmpty(path)) {
      path = "";
    } else {
      path += ":";
    }

    path += rayConfig.libraryPath.stream().collect(Collectors.joining(":"));

    // This is a hack to reset library path at runtime,
    // see https://stackoverflow.com/questions/15409223/.
    System.setProperty("java.library.path", path);
    //set sys_paths to null so that java.library.path will be re-evalueted next time it is needed
    final Field sysPathsField;
    try {
      sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
      sysPathsField.setAccessible(true);
      sysPathsField.set(null, null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      e.printStackTrace();
      LOGGER.error("Failed to set library path.", e);
    }
  }

  @Override
  public void start() throws Exception {
    // Load native libraries.
    try {
      resetLibraryPath();
      System.loadLibrary("raylet_library_java");
      System.loadLibrary("plasma_java");
    } catch (Exception e) {
      LOGGER.error("Failed to load native libraries.", e);
      throw e;
    }

    if (rayConfig.getRedisAddress() == null) {
      manager = new RunManager(rayConfig);
      manager.startRayProcesses(true);
    }
    kvStore = new RedisClient(rayConfig.getRedisAddress());

    objectStoreProxy = new ObjectStoreProxy(this, rayConfig.objectStoreSocketName);

    rayletClient = new RayletClientImpl(
        rayConfig.rayletSocketName,
        workerContext.getCurrentWorkerId(),
        rayConfig.workerMode == WorkerMode.WORKER,
        workerContext.getCurrentTask().taskId
    );

    // register
    registerWorker();

    LOGGER.info("RayNativeRuntime started with store {}, raylet {}",
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName);
  }

  @Override
  public void shutdown() {
    if (null != manager) {
      manager.cleanup();
    }
  }

  private void registerWorker() {
    Map<String, String> workerInfo = new HashMap<>();
    String workerId = new String(workerContext.getCurrentWorkerId().getBytes());
    if (rayConfig.workerMode == WorkerMode.DRIVER) {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("driver_id", workerId);
      workerInfo.put("start_time", String.valueOf(System.currentTimeMillis()));
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      workerInfo.put("name", System.getProperty("user.dir"));
      //TODO: worker.redis_client.hmset(b"Drivers:" + worker.workerId, driver_info)
      kvStore.hmset("Drivers:" + workerId, workerInfo);
    } else {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      //TODO: b"Workers:" + worker.workerId,
      kvStore.hmset("Workers:" + workerId, workerInfo);
    }
  }

}
