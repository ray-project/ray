package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.ray.api.RayActor;
import org.ray.api.RayPyActor;
import org.ray.api.id.UniqueId;

public class RayActorImpl implements RayActor, RayPyActor, Externalizable {
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
    return new UniqueId(getActorId(nativeActorHandle));
  }

  @Override
  public UniqueId getHandleId() {
    return new UniqueId(getActorHandleId(nativeActorHandle));
  }

  public WorkerLanguage getLanguage() {
    return WorkerLanguage.fromInteger(getLanguage(nativeActorHandle));
  }

  @Override
  public String getModuleName() {
    Preconditions.checkState(getLanguage() == WorkerLanguage.PYTHON);
    return getActorDefinitionDescriptor(nativeActorHandle).get(0);
  }

  @Override
  public String getClassName() {
    Preconditions.checkState(getLanguage() == WorkerLanguage.PYTHON);
    return getActorDefinitionDescriptor(nativeActorHandle).get(1);
  }

  public RayActorImpl fork() {
    return new RayActorImpl(fork(nativeActorHandle));
  }

  public byte[] Binary() {
    return serialize(nativeActorHandle);
  }

  public static RayActorImpl fromBinary(byte[] binary) {
    return new RayActorImpl(deserialize(binary));
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(Binary());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    nativeActorHandle = deserialize((byte[]) in.readObject());
  }

  private static native long fork(long nativeActorHandle);

  private static native byte[] getActorId(long nativeActorHandle);

  private static native byte[] getActorHandleId(long nativeActorHandle);

  private static native int getLanguage(long nativeActorHandle);

  private static native List<String> getActorDefinitionDescriptor(long nativeActorHandle);

  private static native byte[] serialize(long nativeActorHandle);

  private static native long deserialize(byte[] data);
}
