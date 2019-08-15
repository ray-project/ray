package org.ray.runtime;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.ray.api.RayPyActor;
import org.ray.api.id.ActorId;

public class RayPyActorImpl extends RayActorImpl implements RayPyActor {

  public static final RayPyActorImpl NIL = new RayPyActorImpl(ActorId.NIL, null, null);

  /**
   * Module name of the Python actor class.
   */
  private String moduleName;

  /**
   * Name of the Python actor class.
   */
  private String className;

  // Note that this empty constructor must be public
  // since it'll be needed when deserializing.
  public RayPyActorImpl() {}

  public RayPyActorImpl(ActorId id, String moduleName, String className) {
    super(id);
    this.moduleName = moduleName;
    this.className = className;
  }

  @Override
  public String getModuleName() {
    return moduleName;
  }

  @Override
  public String getClassName() {
    return className;
  }

  public RayPyActorImpl fork() {
    RayPyActorImpl ret = new RayPyActorImpl();
    ret.id = this.id;
    ret.taskCounter = 0;
    ret.numForks = 0;
    ret.taskCursor = this.taskCursor;
    ret.moduleName = this.moduleName;
    ret.className = this.className;
    ret.handleId = this.computeNextActorHandleId();
    newActorHandles.add(ret.handleId);
    return ret;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeObject(this.moduleName);
    out.writeObject(this.className);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    this.moduleName = (String) in.readObject();
    this.className = (String) in.readObject();
  }

}

