package org.ray.runtime;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.util.Sha1Digestor;

public class RayActorImpl<T> implements RayActor<T>, Externalizable {

  public static final RayActorImpl NIL = new RayActorImpl();

  /**
   * Id of this actor.
   */
  protected ActorId id;
  /**
   * Handle id of this actor.
   */
  protected UniqueId handleId;
  /**
   * The number of tasks that have been invoked on this actor.
   */
  protected int taskCounter;
  /**
   * The unique id of the last return of the last task.
   * It's used as a dependency for the next task.
   */
  protected ObjectId taskCursor;
  /**
   * The number of times that this actor handle has been forked.
   * It's used to make sure ids of actor handles are unique.
   */
  protected int numForks;

  /**
   * The new actor handles that were created from this handle
   * since the last task on this handle was submitted. This is
   * used to garbage-collect dummy objects that are no longer
   * necessary in the backend.
   */
  protected List<UniqueId> newActorHandles;

  public RayActorImpl() {
    this(ActorId.NIL, UniqueId.NIL);
  }

  public RayActorImpl(ActorId id) {
    this(id, UniqueId.NIL);
  }

  public RayActorImpl(ActorId id, UniqueId handleId) {
    this.id = id;
    this.handleId = handleId;
    this.taskCounter = 0;
    this.taskCursor = null;
    this.newActorHandles = new ArrayList<>();
    numForks = 0;
  }

  @Override
  public ActorId getId() {
    return id;
  }

  @Override
  public UniqueId getHandleId() {
    return handleId;
  }

  public void setTaskCursor(ObjectId taskCursor) {
    this.taskCursor = taskCursor;
  }

  public List<UniqueId> getNewActorHandles() {
    return this.newActorHandles;
  }

  public void clearNewActorHandles() {
    this.newActorHandles.clear();
  }

  public ObjectId getTaskCursor() {
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
    ret.taskCursor = this.taskCursor;
    ret.handleId = this.computeNextActorHandleId();
    newActorHandles.add(ret.handleId);
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
    this.id = (ActorId) in.readObject();
    this.handleId = (UniqueId) in.readObject();
    this.taskCursor = (ObjectId) in.readObject();
    this.taskCounter = (int) in.readObject();
    this.numForks = (int) in.readObject();
  }
}
