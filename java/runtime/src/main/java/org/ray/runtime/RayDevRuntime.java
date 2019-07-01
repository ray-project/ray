package org.ray.runtime;

import org.ray.runtime.config.RayConfig;
import org.ray.runtime.objectstore.MockObjectInterface;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.MockRayletClient;

public class RayDevRuntime extends AbstractRayRuntime {

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private MockObjectInterface objectInterface;

  @Override
  public void start() {
    // Reset library path at runtime.
    resetLibraryPath();

    objectInterface = new MockObjectInterface(workerContext);
    objectStoreProxy = new ObjectStoreProxy(workerContext, objectInterface);
    rayletClient = new MockRayletClient(this, rayConfig.numberExecThreadsForDevRuntime);
  }

  @Override
  public void shutdown() {
    rayletClient.destroy();
  }

  public MockObjectInterface getObjectInterface() {
    return objectInterface;
  }

  @Override
  public Worker getWorker() {
    return ((MockRayletClient) rayletClient).getCurrentWorker();
  }
}
