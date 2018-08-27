package org.ray.core;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.ray.api.RayActor;
import org.ray.api.UniqueID;
import org.ray.util.Sha1Digestor;

public final class RayActorImpl<T> implements RayActor<T>, Externalizable {

  public static final RayActorImpl NIL = new RayActorImpl();

  private UniqueID id;
  private UniqueID handleId;
  private int taskCounter;
  private UniqueID taskCursor;
  private int numForks;

  public RayActorImpl() {
    this(UniqueID.NIL, UniqueID.NIL);
  }

  public RayActorImpl(UniqueID id) {
    this(id, UniqueID.NIL);
  }

  public RayActorImpl(UniqueID id, UniqueID handleId) {
    this.id = id;
    this.handleId = handleId;
    taskCounter = 0;
    taskCursor = null;
    numForks = 0;
  }

  @Override
  public UniqueID getId() {
    return id;
  }

  @Override
  public UniqueID getHandleId() {
    return handleId;
  }

  @Override
  public UniqueID getLastTaskId() {
    return taskCursor;
  }

  @Override
  public int getTaskCounter() {
    return taskCounter;
  }

  @Override
  public void onSubmittingTask(UniqueID taskId) {
    taskCursor = taskId;
    taskCounter += 1;
  }

  private UniqueID computeNextActorHandleId() {
    byte[] bytes = Sha1Digestor.digest(handleId.getBytes(), ++numForks);
    return new UniqueID(bytes);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(this.id);
    out.writeObject(this.computeNextActorHandleId());
    out.writeObject(this.taskCursor);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.id = (UniqueID) in.readObject();
    this.handleId = (UniqueID) in.readObject();
    this.taskCursor = (UniqueID) in.readObject();
  }
}
