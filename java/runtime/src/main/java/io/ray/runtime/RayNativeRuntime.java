package io.ray.runtime;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.context.NativeWorkerContext;
import io.ray.runtime.exception.RayException;
import io.ray.runtime.exception.RayIntentionalSystemExitException;
import io.ray.runtime.gcs.GcsClient;
import io.ray.runtime.gcs.GcsClientOptions;
import io.ray.runtime.gcs.RedisClient;
import io.ray.runtime.generated.Common.WorkerType;
import io.ray.runtime.generated.Gcs.JobConfig;
import io.ray.runtime.object.NativeObjectStore;
import io.ray.runtime.runner.RunManager;
import io.ray.runtime.task.NativeTaskExecutor;
import io.ray.runtime.task.NativeTaskSubmitter;
import io.ray.runtime.task.TaskExecutor;
import io.ray.runtime.util.BinaryFileUtil;
import io.ray.runtime.util.JniUtils;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native runtime for cluster mode.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  private RunManager manager = null;

  /**
   * In Java, GC runs in a standalone thread, and we can't control the exact
   * timing of garbage collection. By using this lock, when
   * {@link NativeObjectStore#nativeRemoveLocalReference} is executing, the core
   * worker will not be shut down, therefore it guarantees some kind of
   * thread-safety. Note that this guarantee only works for driver.
   */
  private final ReadWriteLock shutdownLock = new ReentrantReadWriteLock();


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

    JniUtils.loadLibrary(BinaryFileUtil.CORE_WORKER_JAVA_LIBRARY, true);
    LOGGER.debug("Native libraries loaded.");
    try {
      FileUtils.forceMkdir(new File(rayConfig.logDir));
    } catch (IOException e) {
      throw new RuntimeException("Failed to create the log directory.", e);
    }
  }

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
    loadConfigFromGcs(rayConfig);
  }

  private static void loadConfigFromGcs(RayConfig rayConfig) {
    if (rayConfig.getRedisAddress() != null) {
      GcsClient tempGcsClient =
          new GcsClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);
      for (Map.Entry<String, String> entry :
          tempGcsClient.getInternalConfig().entrySet()) {
        rayConfig.rayletConfigParameters.put(entry.getKey(), entry.getValue());
      }

      if (rayConfig.workerMode == WorkerType.DRIVER) {
        // Keep this method logic in sync with `services.get_address_info_from_redis_helper`
        int numRetries = 5;
        int retryCount = 0;
        boolean configLoaded = false;
        while (retryCount++ < numRetries) {
          for (NodeInfo nodeInfo : tempGcsClient.getAllNodeInfo()) {
            if (rayConfig.nodeIp.equals(nodeInfo.nodeAddress) ||
                (nodeInfo.nodeAddress.equals("127.0.0.1") &&
                    rayConfig.nodeIp.equals(rayConfig.getRedisAddress()))) {
              rayConfig.objectStoreSocketName = nodeInfo.objectStoreSocketName;
              rayConfig.rayletSocketName = nodeInfo.rayletSocketName;
              rayConfig.nodeManagerPort = nodeInfo.nodeManagerPort;
              configLoaded = true;
              break;
            }
          }
          if (!configLoaded) {
            LOGGER.warn("Some processes that the driver needs to connect to have " +
                "not registered with Redis, so retrying. Have you run " +
                "'ray start' on this node?");
            try {
              TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          } else {
            break;
          }
        }
        if (!configLoaded) {
          throw new RayException("Some processes that the driver needs to connect to have " +
              "not registered with Redis. Have you run 'ray start' on this node?");
        }
      }
    }
  }

  @Override
  public void start() {
    if (rayConfig.getRedisAddress() == null) {
      manager = new RunManager(rayConfig);
      manager.startRayProcesses(true);
    }

    gcsClient = new GcsClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);

    if (rayConfig.getJobId() == JobId.NIL) {
      rayConfig.setJobId(gcsClient.nextJobId());
    }
    int numWorkersPerProcess =
        rayConfig.workerMode == WorkerType.DRIVER ? 1 : rayConfig.numWorkersPerProcess;

    byte[] serializedJobConfig = null;
    if (rayConfig.workerMode == WorkerType.DRIVER) {
      JobConfig.Builder jobConfigBuilder =
          JobConfig.newBuilder()
              .setNumJavaWorkersPerProcess(rayConfig.numWorkersPerProcess)
              .addAllJvmOptions(rayConfig.jvmOptionsForJavaWorker)
              .putAllWorkerEnv(rayConfig.workerEnv)
              .addAllCodeSearchPath(rayConfig.codeSearchPath);
      serializedJobConfig = jobConfigBuilder.build().toByteArray();
    }

    // TODO(qwang): Get object_store_socket_name and raylet_socket_name from Redis.
    nativeInitialize(rayConfig.workerMode.getNumber(),
        rayConfig.nodeIp, rayConfig.getNodeManagerPort(),
        rayConfig.workerMode == WorkerType.DRIVER ? System.getProperty("user.dir") : "",
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName,
        (rayConfig.workerMode == WorkerType.DRIVER ? rayConfig.getJobId() : JobId.NIL).getBytes(),
        new GcsClientOptions(rayConfig), numWorkersPerProcess,
        rayConfig.logDir, rayConfig.rayletConfigParameters, serializedJobConfig);

    taskExecutor = new NativeTaskExecutor(this);
    workerContext = new NativeWorkerContext();
    objectStore = new NativeObjectStore(workerContext, shutdownLock);
    taskSubmitter = new NativeTaskSubmitter();

    LOGGER.debug("RayNativeRuntime started with store {}, raylet {}",
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName);
  }

  @Override
  public void shutdown() {
    // `shutdown` won't be called concurrently, but the lock is also used in `NativeObjectStore`.
    // When an object is garbage collected, the object will be unregistered from core worker.
    // Since GC runs in a separate thread, we need to make sure that core worker is available
    // when `NativeObjectStore` is accessing core worker in the GC thread.
    Lock writeLock = shutdownLock.writeLock();
    writeLock.lock();
    try {
      if (rayConfig.workerMode == WorkerType.DRIVER) {
        nativeShutdown();
        if (null != manager) {
          manager.cleanup();
          manager = null;
        }
      }
      if (null != gcsClient) {
        gcsClient.destroy();
        gcsClient = null;
      }
      RayConfig.reset();
      LOGGER.debug("RayNativeRuntime shutdown");
    } finally {
      writeLock.unlock();
    }
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
    nativeSetResource(resourceName, capacity, nodeId.getBytes());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends BaseActorHandle> Optional<T> getActor(String name, boolean global) {
    if (name.isEmpty()) {
      return Optional.empty();
    }
    byte[] actorIdBytes = nativeGetActorIdOfNamedActor(name, global);
    ActorId actorId = ActorId.fromBytes(actorIdBytes);
    if (actorId.isNil()) {
      return Optional.empty();
    } else {
      return Optional.of((T) getActorHandle(actorId));
    }
  }

  @Override
  public void killActor(BaseActorHandle actor, boolean noRestart) {
    nativeKillActor(actor.getId().getBytes(), noRestart);
  }

  @Override
  public Object getAsyncContext() {
    return new AsyncContext(workerContext.getCurrentWorkerId(),
        workerContext.getCurrentClassLoader());
  }

  @Override
  public void setAsyncContext(Object asyncContext) {
    nativeSetCoreWorker(((AsyncContext) asyncContext).workerId.getBytes());
    workerContext.setCurrentClassLoader(((AsyncContext) asyncContext).currentClassLoader);
    super.setAsyncContext(asyncContext);
  }

  @Override
  public void exitActor() {
    if (rayConfig.workerMode != WorkerType.WORKER || runtimeContext.getCurrentActorId().isNil()) {
      throw new RuntimeException("This shouldn't be called on a non-actor worker.");
    }
    LOGGER.info("Actor {} is exiting.", runtimeContext.getCurrentActorId());
    throw new RayIntentionalSystemExitException(
        String.format("Actor %s is exiting.", runtimeContext.getCurrentActorId()));
  }

  @Override
  public void run() {
    Preconditions.checkState(rayConfig.workerMode == WorkerType.WORKER);
    nativeRunTaskExecutor(taskExecutor);
  }

  private static native void nativeInitialize(
      int workerMode, String ndoeIpAddress,
      int nodeManagerPort, String driverName, String storeSocket, String rayletSocket,
      byte[] jobId, GcsClientOptions gcsClientOptions, int numWorkersPerProcess,
      String logDir, Map<String, String> rayletConfigParameters, byte[] serializedJobConfig);

  private static native void nativeRunTaskExecutor(TaskExecutor taskExecutor);

  private static native void nativeShutdown();

  private static native void nativeSetResource(String resourceName, double capacity, byte[] nodeId);

  private static native void nativeKillActor(byte[] actorId, boolean noRestart);

  private static native byte[] nativeGetActorIdOfNamedActor(String actorName, boolean global);

  private static native void nativeSetCoreWorker(byte[] workerId);

  static class AsyncContext {

    public final UniqueId workerId;
    public final ClassLoader currentClassLoader;

    AsyncContext(UniqueId workerId, ClassLoader currentClassLoader) {
      this.workerId = workerId;
      this.currentClassLoader = currentClassLoader;
    }
  }
}
