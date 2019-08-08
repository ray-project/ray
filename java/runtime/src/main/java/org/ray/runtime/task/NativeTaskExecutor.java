package org.ray.runtime.task;

import com.google.common.base.Preconditions;
import org.ray.api.id.JobId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.context.NativeWorkerContext;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.gcs.GcsClientOptions;
import org.ray.runtime.object.NativeObjectStore;
import org.ray.runtime.object.ObjectStoreProxy;
import org.ray.runtime.raylet.NativeRayletClient;

/**
 * The worker, which pulls tasks from raylet and executes them continuously.
 */
public class NativeTaskExecutor extends TaskExecutor {

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

  public NativeTaskExecutor(WorkerType workerType, AbstractRayRuntime runtime,
      String storeSocket, String rayletSocket, JobId jobId) {
    super(runtime);
    GcsClientOptions gcsClientOptions = new GcsClientOptions(runtime.getRayConfig());
    nativeCoreWorkerPointer = nativeInit(workerType.getNumber(), storeSocket, rayletSocket,
        jobId.getBytes(), gcsClientOptions);
    Preconditions.checkState(nativeCoreWorkerPointer != 0);
    this.rayletClient = new NativeRayletClient(nativeCoreWorkerPointer);
    this.workerContext = new NativeWorkerContext(nativeCoreWorkerPointer);
    this.objectStoreProxy = new ObjectStoreProxy(workerContext,
        new NativeObjectStore(nativeCoreWorkerPointer));
    this.taskSubmitter = new NativeTaskSubmitter(nativeCoreWorkerPointer);
  }

  public void loop() {
    nativeRunCoreWorker(nativeCoreWorkerPointer, this);
  }

  public void destroy() {
    nativeDestroy(nativeCoreWorkerPointer);
  }

  private static native long nativeInit(int workerMode, String storeSocket,
      String rayletSocket, byte[] jobId, GcsClientOptions gcsClientOptions);

  private static native void nativeRunCoreWorker(long nativeCoreWorkerPointer,
      NativeTaskExecutor taskExecutor);

  private static native void nativeDestroy(long nativeWorkerContextPointer);
}
