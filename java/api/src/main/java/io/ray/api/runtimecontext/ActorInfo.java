package io.ray.api.runtimecontext;

import io.ray.api.id.ActorId;

public class ActorInfo {
  public final ActorId actorId;

  public final ActorState state;

  public final long numRestarts;

  public final Address address;

  public final String name;

  public ActorInfo(
      ActorId actorId, ActorState state, long numRestarts, Address address, String name) {
    this.actorId = actorId;
    this.state = state;
    this.numRestarts = numRestarts;
    this.address = address;
    this.name = name;
  }
}
