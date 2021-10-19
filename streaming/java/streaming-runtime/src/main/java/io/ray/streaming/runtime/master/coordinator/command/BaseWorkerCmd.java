package io.ray.streaming.runtime.master.coordinator.command;

import io.ray.api.id.ActorId;
import java.io.Serializable;

public abstract class BaseWorkerCmd implements Serializable {

  public ActorId fromActorId;

  public BaseWorkerCmd() {}

  protected BaseWorkerCmd(ActorId actorId) {
    this.fromActorId = actorId;
  }
}
