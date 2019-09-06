package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.ray.api.id.JobId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.context.NativeWorkerContext;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.gcs.GcsClientOptions;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.object.NativeObjectStore;
import org.ray.runtime.raylet.NativeRayletClient;
import org.ray.runtime.runner.RunManager;
import org.ray.runtime.task.NativeTaskSubmitter;
import org.ray.runtime.task.TaskExecutor;
import org.ray.runtime.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native runtime for cluster mode.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  private RunManager manager = null;

  /**
   * The native pointer of core worker.
   */
  private long nativeCoreWorkerPointer;

  static {
    LOGGER.debug("Loading native libraries.");
    // Load native libraries.
    String[] libraries = new String[]{"core_worker_library_java"};
    for (String library : libraries) {
      String fileName = System.mapLibraryName(library);
      try (FileUtil.TempFile libFile = FileUtil.getTempFileFromResource(fileName)) {
        System.load(libFile.getFile().getAbsolutePath());
      }
      LOGGER.debug("Native libraries loaded.");
    }
    nativeSetup(RayConfig.create().logDir);
    Runtime.getRuntime().addShutdownHook(new Thread(RayNativeRuntime::nativeShutdownHook));
  }

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
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
    nativeCoreWorkerPointer = nativeInitCoreWorker(rayConfig.workerMode.getNumber(),
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName,
        (rayConfig.workerMode == WorkerType.DRIVER ? rayConfig.getJobId() : JobId.NIL).getBytes(),
        new GcsClientOptions(rayConfig));
    Preconditions.checkState(nativeCoreWorkerPointer != 0);

    taskExecutor = new TaskExecutor(this);
    workerContext = new NativeWorkerContext(nativeCoreWorkerPointer);
    objectStore = new NativeObjectStore(workerContext, nativeCoreWorkerPointer);
    taskSubmitter = new NativeTaskSubmitter(nativeCoreWorkerPointer);
    rayletClient = new NativeRayletClient(nativeCoreWorkerPointer);

    // register
    registerWorker();

    LOGGER.info("RayNativeRuntime started with store {}, raylet {}",
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName);
  }

  @Override
  public void shutdown() {
    if (nativeCoreWorkerPointer != 0) {
      nativeDestroyCoreWorker(nativeCoreWorkerPointer);
      nativeCoreWorkerPointer = 0;
    }
    if (null != manager) {
      manager.cleanup();
    }
  }

  public void run() {
    nativeRunTaskExecutor(nativeCoreWorkerPointer, taskExecutor);
  }

  /**
   * Register this worker or driver to GCS.
   */
  private void registerWorker() {
    RedisClient redisClient = new RedisClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);
    Map<String, String> workerInfo = new HashMap<>();
    String workerId = new String(workerContext.getCurrentWorkerId().getBytes());
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

  private static native long nativeInitCoreWorker(int workerMode, String storeSocket,
      String rayletSocket, byte[] jobId, GcsClientOptions gcsClientOptions);

  private static native void nativeRunTaskExecutor(long nativeCoreWorkerPointer,
      TaskExecutor taskExecutor);

  private static native void nativeDestroyCoreWorker(long nativeCoreWorkerPointer);

  private static native void nativeSetup(String logDir);

  private static native void nativeShutdownHook();
}
