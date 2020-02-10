package org.ray.runtime.actor;

import com.google.common.base.Preconditions;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.RayNativeRuntime;
import org.ray.runtime.generated.Common.Language;

/**
 * RayActor abstract language-independent implementation for cluster mode. This is a wrapper class
 * for C++ ActorHandle.
 */
public abstract class NativeRayActor implements RayActor, Externalizable {

  /**
   * Address of core worker.
   */
  long nativeCoreWorkerPointer;
  /**
   * ID of the actor.
   */
  byte[] actorId;

  private Language language;

  NativeRayActor(long nativeCoreWorkerPointer, byte[] actorId, Language language) {
    Preconditions.checkState(nativeCoreWorkerPointer != 0);
    Preconditions.checkState(!ActorId.fromBytes(actorId).isNil());
    this.nativeCoreWorkerPointer = nativeCoreWorkerPointer;
    this.actorId = actorId;
    this.language = language;
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
    return language;
  }

  public boolean isDirectCallActor() {
    return nativeIsDirectCallActor(nativeCoreWorkerPointer, actorId);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(nativeSerialize(nativeCoreWorkerPointer, actorId));
    out.writeObject(language);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    RayRuntime runtime = Ray.internal();
    if (runtime instanceof RayMultiWorkerNativeRuntime) {
      runtime = ((RayMultiWorkerNativeRuntime) runtime).getCurrentRuntime();
    }
    Preconditions.checkState(runtime instanceof RayNativeRuntime);

    nativeCoreWorkerPointer = ((RayNativeRuntime) runtime).getNativeCoreWorkerPointer();
    actorId = nativeDeserialize(nativeCoreWorkerPointer, (byte[]) in.readObject());
    language = (Language) in.readObject();
  }


  @Override
  protected void finalize() {
    // TODO(zhijunfu): do we need to free the ActorHandle in core worker?
  }

  private static native boolean nativeIsDirectCallActor(
      long nativeCoreWorkerPointer, byte[] actorId);

  static native List<String> nativeGetActorCreationTaskFunctionDescriptor(
      long nativeCoreWorkerPointer, byte[] actorId);

  private static native byte[] nativeSerialize(long nativeCoreWorkerPointer, byte[] actorId);

  private static native byte[] nativeDeserialize(long nativeCoreWorkerPointer, byte[] data);
}
