package org.ray.runtime.raylet;

import org.apache.commons.lang3.NotImplementedException;
import org.ray.api.id.UniqueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockRayletClient implements RayletClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(MockRayletClient.class);

  @Override
  public UniqueId prepareCheckpoint(UniqueId actorId) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public void notifyActorResumedFromCheckpoint(UniqueId actorId, UniqueId checkpointId) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    LOGGER.error("Not implemented under SINGLE_PROCESS mode.");
  }
}
