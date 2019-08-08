package org.ray.runtime;

import com.google.common.base.Preconditions;
import org.ray.api.id.JobId;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.gcs.GcsClientOptions;
import org.ray.runtime.objectstore.ObjectInterfaceImpl;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.RayletClientImpl;

/**
 * The worker, which pulls tasks from raylet and executes them continuously.
 */
class WorkerImpl extends AbstractWorker {

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

  WorkerImpl(WorkerType workerType, AbstractRayRuntime runtime,
      String storeSocket, String rayletSocket, JobId jobId) {
    super(runtime);
    GcsClientOptions gcsClientOptions = new GcsClientOptions(runtime.getRayConfig());
    nativeCoreWorkerPointer = nativeInit(workerType.getNumber(), storeSocket, rayletSocket,
        jobId.getBytes(), gcsClientOptions);
    Preconditions.checkState(nativeCoreWorkerPointer != 0);
    this.rayletClient = new RayletClientImpl(nativeCoreWorkerPointer);
    this.workerContext = new WorkerContextImpl(nativeCoreWorkerPointer);
    this.objectStoreProxy = new ObjectStoreProxy(workerContext,
        new ObjectInterfaceImpl(nativeCoreWorkerPointer));
    this.taskInterface = new TaskInterfaceImpl(nativeCoreWorkerPointer);
  }

  public void loop() {
    nativeRunCoreWorker(nativeCoreWorkerPointer, this);
  }

  public void destroy() {
    nativeDestroy(nativeCoreWorkerPointer);
  }

  private static native long nativeInit(int workerMode, String storeSocket,
      String rayletSocket, byte[] jobId, GcsClientOptions gcsClientOptions);

  private static native void nativeRunCoreWorker(long nativeCoreWorkerPointer, WorkerImpl worker);

  private static native void nativeDestroy(long nativeWorkerContextPointer);
}
