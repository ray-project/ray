package org.ray.runtime;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.ray.api.RayActor;
import org.ray.api.id.UniqueId;
import org.ray.runtime.util.Sha1Digestor;

public final class RayActorImpl<T> implements RayActor<T>, Externalizable {

  public static final RayActorImpl NIL = new RayActorImpl();

  private UniqueId id;
  private UniqueId handleId;
  /**
   * The number of tasks that have been invoked on this actor.
   */
  private int taskCounter;
  /**
   * The unique id of the last return of the last task.
   * It's used as a dependency for the next task.
   */
  private UniqueId taskCursor;
  /**
   * The number of times that this actor handle has been forked.
   * It's used to make sure ids of actor handles are unique.
   */
  private int numForks;

  public RayActorImpl() {
    this(UniqueId.NIL, UniqueId.NIL);
  }

  public RayActorImpl(UniqueId id) {
    this(id, UniqueId.NIL);
  }

  public RayActorImpl(UniqueId id, UniqueId handleId) {
    this.id = id;
    this.handleId = handleId;
    this.taskCounter = 0;
    this.taskCursor = null;
    numForks = 0;
  }

  @Override
  public UniqueId getId() {
    return id;
  }

  @Override
  public UniqueId getHandleId() {
    return handleId;
  }

  public void setTaskCursor(UniqueId taskCursor) {
    this.taskCursor = taskCursor;
  }

  public UniqueId getTaskCursor() {
    return taskCursor;
  }

  public int increaseTaskCounter() {
    return taskCounter++;
  }

  public RayActorImpl<T> fork() {
    RayActorImpl<T> ret = new RayActorImpl<>();
    ret.id = this.id;
    ret.taskCounter = 0;
    ret.numForks = 0;
    ret.taskCursor = this.id;
    ret.handleId = this.computeNextActorHandleId();
    return ret;
  }

  protected UniqueId computeNextActorHandleId() {
    byte[] bytes = Sha1Digestor.digest(handleId.getBytes(), ++numForks);
    return new UniqueId(bytes);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(this.id);
    out.writeObject(this.handleId);
    out.writeObject(this.taskCursor);
    out.writeObject(this.taskCounter);
    out.writeObject(this.numForks);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.id = (UniqueId) in.readObject();
    this.handleId = (UniqueId) in.readObject();
    this.taskCursor = (UniqueId) in.readObject();
    this.taskCounter = (int) in.readObject();
    this.numForks = (int) in.readObject();
  }
}
