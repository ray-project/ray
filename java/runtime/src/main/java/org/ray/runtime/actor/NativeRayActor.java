package org.ray.runtime.actor;

import com.google.common.base.Preconditions;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.ray.api.BaseActor;
import org.ray.api.Ray;
import org.ray.api.id.ActorId;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.RayNativeRuntime;
import org.ray.runtime.generated.Common.Language;

/**
 * Abstract and language-independent implementation of actor handle for cluster mode. This is a
 * wrapper class for C++ ActorHandle.
 */
public abstract class NativeRayActor implements BaseActor, Externalizable {

  /**
   * Address of core worker.
   */
  long nativeCoreWorkerPointer;
  /**
   * ID of the actor.
   */
  byte[] actorId;

  NativeRayActor(long nativeCoreWorkerPointer, byte[] actorId) {
    Preconditions.checkState(nativeCoreWorkerPointer != 0);
    Preconditions.checkState(!ActorId.fromBytes(actorId).isNil());
    this.nativeCoreWorkerPointer = nativeCoreWorkerPointer;
    this.actorId = actorId;
  }

  /**
   * Required by FST
   */
  NativeRayActor() {
  }

  public static NativeRayActor create(long nativeCoreWorkerPointer, byte[] actorId,
                                      Language language) {
    Preconditions.checkState(nativeCoreWorkerPointer != 0);
    switch (language) {
      case JAVA:
        return new NativeRayJavaActor(nativeCoreWorkerPointer, actorId);
      case PYTHON:
        return new NativeRayPyActor(nativeCoreWorkerPointer, actorId);
      default:
        throw new IllegalStateException("Unknown actor handle language: " + language);
    }
  }

  @Override
  public ActorId getId() {
    return ActorId.fromBytes(actorId);
  }

  public Language getLanguage() {
    return Language.forNumber(nativeGetLanguage(nativeCoreWorkerPointer, actorId));
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(toBytes());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    nativeCoreWorkerPointer = getNativeCoreWorkerPointer();
    actorId = nativeDeserialize(nativeCoreWorkerPointer, (byte[]) in.readObject());
  }

  /**
   * Serialize this actor handle to bytes.
   *
   * @return  the bytes of the actor handle
   */
  public byte[] toBytes() {
    return nativeSerialize(nativeCoreWorkerPointer, actorId);
  }

  /**
   * Deserialize an actor handle from bytes.
   *
   * @return  the bytes of an actor handle
   */
  public static NativeRayActor fromBytes(byte[] bytes) {
    long nativeCoreWorkerPointer = getNativeCoreWorkerPointer();
    byte[] actorId = nativeDeserialize(nativeCoreWorkerPointer, bytes);
    Language language = Language.forNumber(nativeGetLanguage(nativeCoreWorkerPointer, actorId));
    Preconditions.checkNotNull(language);
    return create(nativeCoreWorkerPointer, actorId, language);
  }

  private static long getNativeCoreWorkerPointer() {
    RayRuntime runtime = Ray.internal();
    if (runtime instanceof RayMultiWorkerNativeRuntime) {
      runtime = ((RayMultiWorkerNativeRuntime) runtime).getCurrentRuntime();
    }
    Preconditions.checkState(runtime instanceof RayNativeRuntime);

    return ((RayNativeRuntime) runtime).getNativeCoreWorkerPointer();
  }

  @Override
  protected void finalize() {
    // TODO(zhijunfu): do we need to free the ActorHandle in core worker?
  }

  private static native int nativeGetLanguage(
      long nativeCoreWorkerPointer, byte[] actorId);

  static native List<String> nativeGetActorCreationTaskFunctionDescriptor(
      long nativeCoreWorkerPointer, byte[] actorId);

  private static native byte[] nativeSerialize(long nativeCoreWorkerPointer, byte[] actorId);

  private static native byte[] nativeDeserialize(long nativeCoreWorkerPointer, byte[] data);
}
