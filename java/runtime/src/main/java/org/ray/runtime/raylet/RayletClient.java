package org.ray.runtime.raylet;

import org.ray.api.id.UniqueId;

/**
 * Client to the Raylet backend.
 */
public interface RayletClient {

  UniqueId prepareCheckpoint(UniqueId actorId);

  void notifyActorResumedFromCheckpoint(UniqueId actorId, UniqueId checkpointId);

  void setResource(String resourceName, double capacity, UniqueId nodeId);
}
