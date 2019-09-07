package org.ray.runtime.raylet;

import org.apache.commons.lang3.NotImplementedException;
import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raylet client for local mode.
 */
public class LocalModeRayletClient implements RayletClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalModeRayletClient.class);

  @Override
  public UniqueId prepareCheckpoint(ActorId actorId) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public void notifyActorResumedFromCheckpoint(ActorId actorId, UniqueId checkpointId) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    LOGGER.error("Not implemented under SINGLE_PROCESS mode.");
  }
}
