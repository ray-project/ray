package io.ray.streaming.runtime.master.coordinator.command;

import java.io.Serializable;

import io.ray.api.id.ActorId;

public abstract class BaseWorkerCmd implements Serializable {

  public ActorId fromActorId;

  public BaseWorkerCmd() {
  }

  protected BaseWorkerCmd(ActorId actorId) {
    this.fromActorId = actorId;
  }

}
