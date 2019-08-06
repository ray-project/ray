package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import org.ray.api.id.JobId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.runner.RunManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native runtime for cluster mode.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  private RunManager manager = null;

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

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
    nativeStartRayLog(rayConfig.logDir);
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

  @Override
  public void start() {
    // Reset library path at runtime.
    resetLibraryPath();

    if (rayConfig.getRedisAddress() == null) {
      manager = new RunManager(rayConfig);
      manager.startRayProcesses(true);
    }

    gcsClient = new GcsClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);

    if (rayConfig.getJobId() == JobId.NIL) {
      rayConfig.setJobId(gcsClient.nextJobId());
    }
    // TODO(qwang): Get object_store_socket_name and raylet_socket_name from Redis.
    worker = new WorkerImpl(rayConfig.workerMode, this,
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName,
        rayConfig.workerMode == WorkerType.DRIVER ? rayConfig.getJobId() : JobId.NIL);

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
    ((WorkerImpl)worker).destroy();
    nativeShutdownRayLog();
  }

  public void loop() {
    ((WorkerImpl)worker).loop();
  }

  /**
   * Register this worker or driver to GCS.
   */
  private void registerWorker() {
    RedisClient redisClient = new RedisClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);
    Map<String, String> workerInfo = new HashMap<>();
    String workerId = new String(worker.getWorkerContext().getCurrentWorkerId().getBytes());
    if (rayConfig.workerMode == WorkerType.DRIVER) {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("driver_id", workerId);
      workerInfo.put("start_time", String.valueOf(System.currentTimeMillis()));
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      workerInfo.put("name", System.getProperty("user.dir"));
      //TODO: worker.redis_client.hmset(b"Drivers:" + worker.workerId, driver_info)
      redisClient.hmset("Drivers:" + workerId, workerInfo);
    } else {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      //TODO: b"Workers:" + worker.workerId,
      redisClient.hmset("Workers:" + workerId, workerInfo);
    }
  }

  private static native void nativeStartRayLog(String logDir);
  private static native void nativeShutdownRayLog();
}
