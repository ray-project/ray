package io.ray.runtime.actor;

import io.ray.api.ActorHandle;
import io.ray.api.id.ActorId;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/** Implementation of actor handle for local mode. */
public class LocalModeActorHandle implements ActorHandle, Externalizable {

  private ActorId actorId;

  public LocalModeActorHandle(ActorId actorId) {
    this.actorId = actorId;
  }

  /** Required by FST. */
  public LocalModeActorHandle() {}

  @Override
  public ActorId getId() {
    return actorId;
  }

  public LocalModeActorHandle copy() {
    return new LocalModeActorHandle(this.actorId);
  }

  @Override
  public synchronized void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(actorId);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    actorId = (ActorId) in.readObject();
  }
}
