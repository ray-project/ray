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
  /**
   * The number of tasks that have been invoked on this actor.
   */
  private int taskCounter;
  /**
   * The unique id of the last return of the last task.
   * It's used as a dependency for the next task.
   */
  private UniqueID taskCursor;
  /**
   * The number of times that this actor handle has been forked.
   * It's used to make sure ids of actor handles are unique.
   */
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
    this.taskCounter = 0;
    this.taskCursor = null;
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

  public void setTaskCursor(UniqueID taskCursor) {
    this.taskCursor = taskCursor;
  }

  public UniqueID getTaskCursor() {
    return taskCursor;
  }

  public int increaseTaskCounter() {
    return taskCounter++;
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
