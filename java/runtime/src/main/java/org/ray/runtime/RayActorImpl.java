package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.ray.api.RayPyActor;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.Language;

public class RayActorImpl extends AbstractRayActor implements RayPyActor, Externalizable {

  /**
   * Address of native actor handle.
   */
  private long nativeActorHandle;

  public RayActorImpl(long nativeActorHandle) {
    Preconditions.checkState(nativeActorHandle != 0);
    this.nativeActorHandle = nativeActorHandle;
  }

  public RayActorImpl() {
  }

  public long getNativeActorHandle() {
    return nativeActorHandle;
  }

  @Override
  public UniqueId getId() {
    return new UniqueId(nativeGetActorId(nativeActorHandle));
  }

  @Override
  public UniqueId getHandleId() {
    return new UniqueId(nativeGetActorHandleId(nativeActorHandle));
  }

  @Override
  public Language getLanguage() {
    return Language.forNumber(nativeGetLanguage(nativeActorHandle));
  }

  @Override
  public String getModuleName() {
    Preconditions.checkState(getLanguage() == Language.PYTHON);
    return nativeGetActorCreationTaskFunctionDescriptor(nativeActorHandle).get(0);
  }

  @Override
  public String getClassName() {
    Preconditions.checkState(getLanguage() == Language.PYTHON);
    return nativeGetActorCreationTaskFunctionDescriptor(nativeActorHandle).get(1);
  }

  public RayActorImpl fork() {
    return new RayActorImpl(nativeFork(nativeActorHandle));
  }

  public byte[] Binary() {
    return nativeSerialize(nativeActorHandle);
  }

  public static AbstractRayActor fromBinary(byte[] binary) {
    return new RayActorImpl(nativeDeserialize(binary));
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(Binary());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    nativeActorHandle = nativeDeserialize((byte[]) in.readObject());
  }

  @Override
  protected void finalize() {
    nativeFree(nativeActorHandle);
  }

  private static native long nativeFork(long nativeActorHandle);

  private static native byte[] nativeGetActorId(long nativeActorHandle);

  private static native byte[] nativeGetActorHandleId(long nativeActorHandle);

  private static native int nativeGetLanguage(long nativeActorHandle);

  private static native List<String> nativeGetActorCreationTaskFunctionDescriptor(
      long nativeActorHandle);

  private static native byte[] nativeSerialize(long nativeActorHandle);

  private static native long nativeDeserialize(byte[] data);

  private static native void nativeFree(long nativeActorHandle);
}
