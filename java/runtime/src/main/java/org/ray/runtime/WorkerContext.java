package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.raylet.RayletClientImpl;
import org.ray.runtime.task.TaskSpec;

/**
 * This is a wrapper class for worker context of core worker.
 */
public class WorkerContext {

  /**
   * The native pointer of worker context of core worker.
   */
  private final long nativeWorkerContextPointer;

  private ClassLoader currentClassLoader;

  /**
   * The ID of main thread which created the worker context.
   */
  private long mainThreadId;

  /**
   * The run-mode of this worker.
   */
  private RunMode runMode;

  public WorkerContext(WorkerType workerType, JobId jobId, RunMode runMode) {
    this.nativeWorkerContextPointer = nativeCreateWorkerContext(workerType.getNumber(), jobId.getBytes());
    mainThreadId = Thread.currentThread().getId();
    this.runMode = runMode;
    currentClassLoader = null;
  }

  public long getNativeWorkerContext() {
    return nativeWorkerContextPointer;
  }

  /**
   * @return For the main thread, this method returns the ID of this worker's current running task;
   * for other threads, this method returns a random ID.
   */
  public TaskId getCurrentTaskId() {
    return TaskId.fromBytes(nativeGetCurrentTaskId(nativeWorkerContextPointer));
  }

  /**
   * Set the current task which is being executed by the current worker. Note, this method can only
   * be called from the main thread.
   */
  public void setCurrentTask(TaskSpec task, ClassLoader classLoader) {
    if (runMode == RunMode.CLUSTER) {
      Preconditions.checkState(
          Thread.currentThread().getId() == mainThreadId,
          "This method should only be called from the main thread."
      );
    }

    Preconditions.checkNotNull(task);
    byte[] taskSpec = RayletClientImpl.convertTaskSpecToProtobuf(task);
    nativeSetCurrentTask(nativeWorkerContextPointer, taskSpec);
    currentClassLoader = classLoader;
  }

  /**
   * Increment the put index and return the new value.
   */
  public int nextPutIndex() {
    return nativeGetNextPutIndex(nativeWorkerContextPointer);
  }

  /**
   * Increment the task index and return the new value.
   */
  public int nextTaskIndex() {
    return nativeGetNextTaskIndex(nativeWorkerContextPointer);
  }

  /**
   * @return The ID of the current worker.
   */
  public UniqueId getCurrentWorkerId() {
    return new UniqueId(nativeGetCurrentWorkerId(nativeWorkerContextPointer));
  }

  /**
   * The ID of the current job.
   */
  public JobId getCurrentJobId() {
    return JobId.fromByteBuffer(nativeGetCurrentJobId(nativeWorkerContextPointer));
  }

  /**
   * @return The class loader which is associated with the current job.
   */
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  /**
   * Get the current task.
   */
  public TaskSpec getCurrentTask() {
    byte[] bytes = nativeGetCurrentTask(nativeWorkerContextPointer);
    if (bytes == null) {
      return null;
    }
    return RayletClientImpl.parseTaskSpecFromProtobuf(bytes);
  }

  public void destroy() {
    nativeDestroy(nativeWorkerContextPointer);
  }

  private static native long nativeCreateWorkerContext(int workerType, byte[] jobId);

  private static native byte[] nativeGetCurrentTaskId(long nativeWorkerContextPointer);

  private static native void nativeSetCurrentTask(long nativeWorkerContextPointer, byte[] taskSpec);

  private static native byte[] nativeGetCurrentTask(long nativeWorkerContextPointer);

  private static native ByteBuffer nativeGetCurrentJobId(long nativeWorkerContextPointer);

  private static native byte[] nativeGetCurrentWorkerId(long nativeWorkerContextPointer);

  private static native int nativeGetNextTaskIndex(long nativeWorkerContextPointer);

  private static native int nativeGetNextPutIndex(long nativeWorkerContextPointer);

  private static native void nativeDestroy(long nativeWorkerContextPointer);
}
