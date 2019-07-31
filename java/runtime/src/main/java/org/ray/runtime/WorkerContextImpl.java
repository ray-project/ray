package org.ray.runtime;

import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import org.ray.api.id.JobId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.TaskSpec;

/**
 * This is a wrapper class for worker context of core worker.
 */
public class WorkerContextImpl implements WorkerContext {

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

  private ClassLoader currentClassLoader;

  public WorkerContextImpl(long nativeCoreWorkerPointer) {
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
  public UniqueId getCurrentActorId() {
    return UniqueId.fromByteBuffer(nativeGetCurrentActorId(nativeCoreWorkerPointer));
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
  public TaskSpec getCurrentTask() {
    byte[] bytes = nativeGetCurrentTask(nativeCoreWorkerPointer);
    if (bytes == null) {
      return null;
    }
    try {
      return TaskSpec.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static native byte[] nativeGetCurrentTask(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentJobId(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentWorkerId(long nativeCoreWorkerPointer);

  private static native ByteBuffer nativeGetCurrentActorId(long nativeCoreWorkerPointer);
}
