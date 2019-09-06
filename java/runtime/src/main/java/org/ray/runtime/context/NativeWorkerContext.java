package org.ray.runtime.context;

import java.nio.ByteBuffer;
import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.TaskType;

/**
 * Worker context for cluster mode. This is a wrapper class for worker context of core worker.
 */
public class NativeWorkerContext implements WorkerContext {

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

  private ClassLoader currentClassLoader;

  public NativeWorkerContext(long nativeCoreWorkerPointer) {
    this.nativeCoreWorkerPointer = nativeCoreWorkerPointer;
  }

  @Override
  public UniqueId getCurrentWorkerId() {
    return UniqueId.fromByteBuffer(nativeGetCurrentWorkerId(nativeCoreWorkerPointer));
  }

  @Override
  public JobId getCurrentJobId() {
    return JobId.fromByteBuffer(nativeGetCurrentJobId(nativeCoreWorkerPointer));
  }

  @Override
  public ActorId getCurrentActorId() {
    return ActorId.fromByteBuffer(nativeGetCurrentActorId(nativeCoreWorkerPointer));
  }

  @Override
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  @Override
  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    if (this.currentClassLoader != currentClassLoader) {
      this.currentClassLoader = currentClassLoader;
    }
  }

  @Override
  public TaskType getCurrentTaskType() {
    return TaskType.forNumber(nativeGetCurrentTaskType(nativeCoreWorkerPointer));
  }

  @Override
  public TaskId getCurrentTaskId() {
    return TaskId.fromByteBuffer(nativeGetCurrentTaskId(nativeCoreWorkerPointer));
  }

  private static native int nativeGetCurrentTaskType(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentTaskId(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentJobId(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentWorkerId(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentActorId(long nativeCoreWorkerPointer);
}
