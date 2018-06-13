package org.ray.api;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.ray.util.Sha1Digestor;

/**
 * Ray actor abstraction.
 */
public class RayActor<T> extends RayObject<T> implements Externalizable {

  private static final long serialVersionUID = 1877485807405645036L;

  private int taskCounter = 0;

  private UniqueID taskCursor = null;

  private UniqueID actorHandleId = UniqueID.nil;

  private int forksNum = 0;

  public RayActor() {
  }

  public RayActor(UniqueID id) {
    super(id);
    this.taskCounter = 1;
  }

  public RayActor(UniqueID id, UniqueID actorHandleId) {
    super(id);
    this.actorHandleId = actorHandleId;
    this.taskCounter = 0;
  }

  public int increaseTaskCounter() {
    return taskCounter++;
  }

  /**
   * Getter method for property <tt>taskCursor</tt>.
   *
   * @return property value of taskCursor
   */
  public UniqueID getTaskCursor() {
    return taskCursor;
  }

  /**
   * Setter method for property <tt>taskCursor</tt>.
   *
   * @param taskCursor value to be assigned to property taskCursor
   */
  public void setTaskCursor(UniqueID taskCursor) {
    this.taskCursor = taskCursor;
  }

  public UniqueID getActorHandleId() {
    return actorHandleId;
  }

  public void setActorHandleId(UniqueID actorHandleId) {
    this.actorHandleId = actorHandleId;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(this.id);
    out.writeObject(this.computeNextActorHandleId());
    out.writeObject(this.taskCursor);
  }

  public UniqueID computeNextActorHandleId() {
    byte[] bytes = Sha1Digestor.digest(actorHandleId.id, ++forksNum);
    return new UniqueID(bytes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    this.id = (UniqueID) in.readObject();
    this.actorHandleId = (UniqueID) in.readObject();
    this.taskCursor = (UniqueID) in.readObject();
  }
}
