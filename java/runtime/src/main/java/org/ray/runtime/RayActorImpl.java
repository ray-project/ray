package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.ray.api.RayActor;

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
    throw new UnsupportedOperationException();
//    out.writeObject(this.id);
//    out.writeObject(this.handleId);
//    out.writeObject(this.taskCursor);
//    out.writeObject(this.taskCounter);
//    out.writeObject(this.numForks);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException();
//    this.id = (UniqueId) in.readObject();
//    this.handleId = (UniqueId) in.readObject();
//    this.taskCursor = (ObjectId) in.readObject();
//    this.taskCounter = (int) in.readObject();
//    this.numForks = (int) in.readObject();
  }

  private static native long fork(long nativeActorHandle);
}
