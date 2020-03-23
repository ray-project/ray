package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.ray.api.BaseActor;
import org.ray.api.id.JobId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.context.NativeWorkerContext;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.gcs.GcsClientOptions;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.object.NativeObjectStore;
import org.ray.runtime.runner.RunManager;
import org.ray.runtime.task.NativeTaskExecutor;
import org.ray.runtime.task.NativeTaskSubmitter;
import org.ray.runtime.task.TaskExecutor;
import org.ray.runtime.util.JniUtils;
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
    // Expose ray ABI symbols which may be depended by other shared
    // libraries such as libstreaming_java.so.
    // See BUILD.bazel:libcore_worker_library_java.so
    final RayConfig rayConfig = RayConfig.getInstance();
    if (rayConfig.getRedisAddress() != null && rayConfig.workerMode == WorkerType.DRIVER) {
      // Fetch session dir from GCS if this is a driver that is connecting to the existing GCS.
      RedisClient client = new RedisClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);
      final String sessionDir = client.get("session_dir", null);
      Preconditions.checkNotNull(sessionDir);
      rayConfig.setSessionDir(sessionDir);
    }

    JniUtils.loadLibrary("core_worker_library_java", true);
    LOGGER.debug("Native libraries loaded.");
    resetLibraryPath(rayConfig);
    try {
      FileUtils.forceMkdir(new File(rayConfig.logDir));
    } catch (IOException e) {
      throw new RuntimeException("Failed to create the log directory.", e);
    }
    nativeSetup(rayConfig.logDir, rayConfig.rayletConfigParameters);
    Runtime.getRuntime().addShutdownHook(new Thread(RayNativeRuntime::nativeShutdownHook));
  }

  private static void resetLibraryPath(RayConfig rayConfig) {
    String separator = System.getProperty("path.separator");
    String libraryPath = String.join(separator, rayConfig.libraryPath);
    JniUtils.resetLibraryPath(libraryPath);
  }

  public RayNativeRuntime(RayConfig rayConfig, FunctionManager functionManager) {
    super(rayConfig, functionManager);
    // Reset library path at runtime.
    resetLibraryPath(rayConfig);

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
        rayConfig.nodeIp, rayConfig.getNodeManagerPort(),
        (rayConfig.workerMode == WorkerType.DRIVER ? rayConfig.getJobId() : JobId.NIL).getBytes(),
        new GcsClientOptions(rayConfig));
    Preconditions.checkState(nativeCoreWorkerPointer != 0);

    workerContext = new NativeWorkerContext(nativeCoreWorkerPointer);
    taskExecutor = new NativeTaskExecutor(nativeCoreWorkerPointer, this);
    objectStore = new NativeObjectStore(workerContext, nativeCoreWorkerPointer);
    taskSubmitter = new NativeTaskSubmitter(nativeCoreWorkerPointer);

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
      manager = null;
    }
    RayConfig.reset();
    LOGGER.info("RayNativeRuntime shutdown");
  }

  // For test purpose only
  public RunManager getRunManager() {
    return manager;
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    Preconditions.checkArgument(Double.compare(capacity, 0) >= 0);
    if (nodeId == null) {
      nodeId = UniqueId.NIL;
    }
    nativeSetResource(nativeCoreWorkerPointer, resourceName, capacity, nodeId.getBytes());
  }

  @Override
  public void killActor(BaseActor actor, boolean noReconstruction) {
    nativeKillActor(nativeCoreWorkerPointer, actor.getId().getBytes(), noReconstruction);
  }

  @Override
  public Object getAsyncContext() {
    return null;
  }

  @Override
  public void setAsyncContext(Object asyncContext) {
  }

  public void run() {
    nativeRunTaskExecutor(nativeCoreWorkerPointer);
  }

  public long getNativeCoreWorkerPointer() {
    return nativeCoreWorkerPointer;
  }

  public TaskExecutor getTaskExecutor() {
    return taskExecutor;
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
      String rayletSocket, String nodeIpAddress, int nodeManagerPort, byte[] jobId,
      GcsClientOptions gcsClientOptions);

  private static native void nativeRunTaskExecutor(long nativeCoreWorkerPointer);

  private static native void nativeDestroyCoreWorker(long nativeCoreWorkerPointer);

  private static native void nativeSetup(String logDir, Map<String, String> rayletConfigParameters);

  private static native void nativeShutdownHook();

  private static native void nativeSetResource(long conn, String resourceName, double capacity,
      byte[] nodeId);

  private static native void nativeKillActor(long nativeCoreWorkerPointer, byte[] actorId,
      boolean noReconstruction);
}
