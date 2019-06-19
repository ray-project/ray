package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.ray.api.RayActor;
import org.ray.api.id.UniqueId;

public class RayActorImpl<T> implements RayActor<T>, Externalizable {

  /**
   * Address of native actor handle.
   */
  private long nativeActorHandle;

  public RayActorImpl() {
    this(0);
  }

  public RayActorImpl(long nativeActorHandle) {
    this.nativeActorHandle = nativeActorHandle;
  }


  @Override
  public UniqueId getId() {
    Preconditions.checkState(nativeActorHandle != 0);
    return new UniqueId(getActorId(nativeActorHandle));
  }

  @Override
  public UniqueId getHandleId() {
    Preconditions.checkState(nativeActorHandle != 0);
    return new UniqueId(getActorHandleId(nativeActorHandle));
  }

  public long getNativeActorHandle() {
    return nativeActorHandle;
  }

  public RayActorImpl<T> fork() {
    Preconditions.checkState(nativeActorHandle != 0);
    return new RayActorImpl<>(fork(nativeActorHandle));
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    Preconditions.checkState(nativeActorHandle != 0);
    out.writeObject(serialize(nativeActorHandle));
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    Preconditions.checkState(nativeActorHandle == 0);
    byte[] data = (byte[]) in.readObject();
    this.nativeActorHandle = deserialize(data);
  }

  private static native long fork(long nativeActorHandle);

  private static native byte[] getActorId(long nativeActorHandle);

  private static native byte[] getActorHandleId(long nativeActorHandle);

  private static native byte[] serialize(long nativeActorHandle);

  private static native long deserialize(byte[] data);
}
