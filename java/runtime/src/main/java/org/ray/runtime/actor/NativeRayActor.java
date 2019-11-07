package org.ray.runtime.actor;

import com.google.common.base.Preconditions;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayPyActor;
import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.RayNativeRuntime;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.generated.Common.Language;

/**
 * RayActor implementation for cluster mode. This is a wrapper class for C++ ActorHandle.
 */
public class NativeRayActor implements RayActor, RayPyActor, Externalizable {

  /**
   * Address of core worker.
   */
  private long nativeCoreWorkerPointer;  
  /**
   * ID of the actor.
   */
  private byte[] actorId;

  public NativeRayActor(long nativeCoreWorkerPointer, byte[] actorId) {
    Preconditions.checkState(nativeCoreWorkerPointer != 0);
    Preconditions.checkState(!ActorId.fromBytes(actorId).isNil());
    this.nativeCoreWorkerPointer = nativeCoreWorkerPointer;
    this.actorId = actorId;
  }

  /**
   * Required by FST
   */
  public NativeRayActor() {
  }

  @Override
  public ActorId getId() {
    return ActorId.fromBytes(actorId);
  }

  public Language getLanguage() {
    return Language.forNumber(nativeGetLanguage(nativeCoreWorkerPointer, actorId));
  }

  public boolean isDirectCallActor() {
    return nativeIsDirectCallActor(nativeCoreWorkerPointer, actorId);
  }

  @Override
  public String getModuleName() {
    Preconditions.checkState(getLanguage() == Language.PYTHON);
    return nativeGetActorCreationTaskFunctionDescriptor(
      nativeCoreWorkerPointer, actorId).get(0);
  }

  @Override
  public String getClassName() {
    Preconditions.checkState(getLanguage() == Language.PYTHON);
    return nativeGetActorCreationTaskFunctionDescriptor(
      nativeCoreWorkerPointer, actorId).get(1);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(nativeSerialize(nativeCoreWorkerPointer, actorId));
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    RayRuntime runtime = Ray.internal();
    if (runtime instanceof RayMultiWorkerNativeRuntime) {
      runtime = ((RayMultiWorkerNativeRuntime) runtime).getCurrentRuntime();
    }
    
    Preconditions.checkState(runtime instanceof RayNativeRuntime);
    nativeCoreWorkerPointer = ((RayNativeRuntime)runtime).getNativeCoreWorkerPointer();

    actorId = nativeDeserialize(nativeCoreWorkerPointer, (byte[]) in.readObject());
  }

  @Override
  protected void finalize() {
    // TODO(zhijunfu): do we need to free the ActorHandle in core worker?
  }

  private static native int nativeGetLanguage(long nativeCoreWorkerPointer, byte[] actorId);

  private static native boolean nativeIsDirectCallActor(long nativeCoreWorkerPointer, byte[] actorId);

  private static native List<String> nativeGetActorCreationTaskFunctionDescriptor(
    long nativeCoreWorkerPointer, byte[] actorId);

  private static native byte[] nativeSerialize(long nativeCoreWorkerPointer, byte[] actorId);

  private static native byte[] nativeDeserialize(long nativeCoreWorkerPointer, byte[] data);
}
