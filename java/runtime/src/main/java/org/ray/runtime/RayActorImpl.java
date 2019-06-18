package org.ray.runtime;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.ray.api.RayActor;
import org.ray.api.id.UniqueId;

public class RayActorImpl<T> implements RayActor<T>, Externalizable {

  public static final RayActorImpl NIL = new RayActorImpl(UniqueId.NIL, 0);

  /**
   * Id of this actor.
   */
  private UniqueId id;

  /**
   * Address of native actor handle.
   */
  private long nativeActorHandle;

  public RayActorImpl() {
    this(UniqueId.NIL, 0);
  }

  public RayActorImpl(UniqueId id, long nativeActorHandle) {
    this.id = id;
    this.nativeActorHandle = nativeActorHandle;
  }

  public RayActorImpl(byte[] idBytes, long nativeActorHandle) {
    this(new UniqueId(idBytes), nativeActorHandle);
  }

  @Override
  public UniqueId getId() {
    return id;
  }

  public long getNativeActorHandle() {
    return nativeActorHandle;
  }

  public RayActorImpl<T> fork() {
    throw new UnsupportedOperationException();
//    RayActorImpl<T> ret = new RayActorImpl<>();
//    ret.id = this.id;
//    ret.taskCounter = 0;
//    ret.numForks = 0;
//    ret.taskCursor = this.taskCursor;
//    ret.handleId = this.computeNextActorHandleId();
//    newActorHandles.add(ret.handleId);
//    return ret;
  }

//  protected UniqueId computeNextActorHandleId() {
//    byte[] bytes = Sha1Digestor.digest(handleId.getBytes(), ++numForks);
//    return new UniqueId(bytes);
//  }

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
}
