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
    out.writeObject(new UniqueId(getActorId(nativeActorHandle)));
    out.writeObject(new UniqueId(getActorHandleId(nativeActorHandle)));
    out.writeObject(new UniqueId(getActorCursor(nativeActorHandle)));
    out.writeObject(getTaskCounter(nativeActorHandle));
    out.writeObject(getNumForks(nativeActorHandle));
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    Preconditions.checkState(nativeActorHandle == 0);
    UniqueId actorId = (UniqueId) in.readObject();
    UniqueId actorHandleId = (UniqueId) in.readObject();
    UniqueId actorCursor = (UniqueId) in.readObject();
    int taskCounter = (int) in.readObject();
    int numForks = (int) in.readObject();
    this.nativeActorHandle = createActor(actorId.getBytes(), actorHandleId.getBytes(),
        actorCursor.getBytes(), taskCounter, numForks);
  }

  private static native long fork(long nativeActorHandle);

  private static native byte[] getActorId(long nativeActorHandle);

  private static native byte[] getActorHandleId(long nativeActorHandle);

  private static native byte[] getActorCursor(long nativeActorHandle);

  private static native int getTaskCounter(long nativeActorHandle);

  private static native int getNumForks(long nativeActorHandle);

  private static native long createActor(byte[] actorId, byte[] actorHandleId, byte[] actorCursor
      , int taskCounter, int numForks);
}
