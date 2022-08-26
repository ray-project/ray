package io.ray.runtime.context;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.runtime.generated.Common.Address;
import io.ray.runtime.generated.Common.TaskType;
import java.nio.ByteBuffer;

/** Worker context for cluster mode. This is a wrapper class for worker context of core worker. */
public class NativeWorkerContext implements WorkerContext {

  private ClassLoader currentClassLoader = null;

  @Override
  public UniqueId getCurrentWorkerId() {
    return UniqueId.fromByteBuffer(nativeGetCurrentWorkerId());
  }

  @Override
  public JobId getCurrentJobId() {
    return JobId.fromByteBuffer(nativeGetCurrentJobId());
  }

  @Override
  public ActorId getCurrentActorId() {
    return ActorId.fromByteBuffer(nativeGetCurrentActorId());
  }

  @Override
  public TaskType getCurrentTaskType() {
    return TaskType.forNumber(nativeGetCurrentTaskType());
  }

  @Override
  public TaskId getCurrentTaskId() {
    return TaskId.fromByteBuffer(nativeGetCurrentTaskId());
  }

  @Override
  public Address getRpcAddress() {
    try {
      return Address.parseFrom(nativeGetRpcAddress());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RuntimeEnv getCurrentRuntimeEnv() {
    String serialized_runtime_env = nativeGetSerializedRuntimeEnv();
    if (serialized_runtime_env == null) {
      return null;
    }
    return RuntimeEnv.deserialize(serialized_runtime_env);
  }

  private static native int nativeGetCurrentTaskType();

  private static native ByteBuffer nativeGetCurrentTaskId();

  private static native ByteBuffer nativeGetCurrentJobId();

  private static native ByteBuffer nativeGetCurrentWorkerId();

  private static native ByteBuffer nativeGetCurrentActorId();

  private static native byte[] nativeGetRpcAddress();

  private static native String nativeGetSerializedRuntimeEnv();
}
