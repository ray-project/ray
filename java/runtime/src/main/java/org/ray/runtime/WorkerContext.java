package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.config.WorkerMode;
import org.ray.runtime.raylet.RayletClientImpl;
import org.ray.runtime.task.TaskSpec;

public class WorkerContext {
  private final long nativeWorkerContext;

  private ClassLoader currentClassLoader;

  /**
   * The ID of main thread which created the worker context.
   */
  private long mainThreadId;

  /**
   * The run-mode of this worker.
   */
  private RunMode runMode;

  public WorkerContext(WorkerMode workerMode, UniqueId jobId, RunMode runMode) {
    this.nativeWorkerContext = nativeCreateWorkerContext(workerMode.ordinal(), jobId.getBytes());
    mainThreadId = Thread.currentThread().getId();
    this.runMode = runMode;
    currentClassLoader = null;
  }

  public long getNativeWorkerContext() {
    return nativeWorkerContext;
  }

  /**
   * @return For the main thread, this method returns the ID of this worker's current running task;
   *     for other threads, this method returns a random ID.
   */
  public TaskId getCurrentTaskId() {
    return new TaskId(nativeGetCurrentTaskId(nativeWorkerContext));
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
    nativeSetCurrentTask(nativeWorkerContext, taskSpec);
    currentClassLoader = classLoader;
  }

  /**
   * Increment the put index and return the new value.
   */
  public int nextPutIndex() {
    return nativeGetNextPutIndex(nativeWorkerContext);
  }

  /**
   * Increment the task index and return the new value.
   */
  public int nextTaskIndex() {
    return nativeGetNextTaskIndex(nativeWorkerContext);
  }

  /**
   * @return The ID of the current worker.
   */
  public UniqueId getCurrentWorkerId() {
    return new UniqueId(nativeGetCurrentWorkerId(nativeWorkerContext));
  }

  /**
   * The ID of the current job.
   */
  public UniqueId getCurrentJobId() {
    return new UniqueId(nativeGetCurrentJobId(nativeWorkerContext));
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
    byte[] bytes = nativeGetCurrentTask(nativeWorkerContext);
    if (bytes == null) {
      return null;
    }
    return RayletClientImpl.parseTaskSpecFromProtobuf(bytes);
  }

  private static native long nativeCreateWorkerContext(int workerType, byte[] jobId);

  private static native byte[] nativeGetCurrentTaskId(long nativeWorkerContext);

  private static native void nativeSetCurrentTask(long nativeWorkerContext, byte[] taskSpec);

  private static native byte[] nativeGetCurrentTask(long nativeWorkerContext);

  private static native byte[] nativeGetCurrentJobId(long nativeWorkerContext);

  private static native byte[] nativeGetCurrentWorkerId(long nativeWorkerContext);

  private static native int nativeGetNextTaskIndex(long nativeWorkerContext);

  private static native int nativeGetNextPutIndex(long nativeWorkerContext);
}
