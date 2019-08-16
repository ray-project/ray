package org.ray.runtime.raylet;

import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;

/**
 * Client to the Raylet backend.
 */
public interface RayletClient {

  UniqueId prepareCheckpoint(ActorId actorId);

  void notifyActorResumedFromCheckpoint(ActorId actorId, UniqueId checkpointId);

  void setResource(String resourceName, double capacity, UniqueId nodeId);
}
