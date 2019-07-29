package org.ray.runtime;

import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.TaskSpec;

/**
 * This is a wrapper class for worker context of core worker.
 */
public class WorkerContext {

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

  private ClassLoader currentClassLoader;

  public WorkerContext(long nativeCoreWorkerPointer) {
    this.nativeCoreWorkerPointer = nativeCoreWorkerPointer;
  }

  /**
   * @return For the main thread, this method returns the ID of this worker's current running task;
   * for other threads, this method returns a random ID.
   */
  public TaskId getCurrentTaskId() {
    return TaskId.fromByteBuffer(nativeGetCurrentTaskId(nativeCoreWorkerPointer));
  }

  /**
   * @return The ID of the current worker.
   */
  public UniqueId getCurrentWorkerId() {
    return UniqueId.fromByteBuffer(nativeGetCurrentWorkerId(nativeCoreWorkerPointer));
  }

  /**
   * The ID of the current job.
   */
  public JobId getCurrentJobId() {
    return JobId.fromByteBuffer(nativeGetCurrentJobId(nativeCoreWorkerPointer));
  }

  /**
   * The ID of the current job.
   */
  public UniqueId getCurrentActorId() {
    return UniqueId.fromByteBuffer(nativeGetCurrentActorId(nativeCoreWorkerPointer));
  }

  /**
   * @return The class loader which is associated with the current job.
   */
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    if (this.currentClassLoader != currentClassLoader) {
      this.currentClassLoader = currentClassLoader;
    }
  }

  /**
   * Get the current task.
   */
  public TaskSpec getCurrentTask() {
    ByteBuffer byteBuffer = nativeGetCurrentTask(nativeCoreWorkerPointer);
    if (byteBuffer == null) {
      return null;
    }
    try {
      return TaskSpec.parseFrom(byteBuffer);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static native ByteBuffer nativeGetCurrentTaskId(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentTask(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentJobId(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentWorkerId(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentActorId(long nativeCoreWorkerPointer);
}
