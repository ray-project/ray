package org.ray.runtime;

import org.ray.runtime.config.RayConfig;
import org.ray.runtime.objectstore.MockObjectStore;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.MockRayletClient;

public class RayDevRuntime extends AbstractRayRuntime {

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private MockObjectStore store;

  @Override
  public void start() {
    store = new MockObjectStore(this);
    objectStoreProxy = new ObjectStoreProxy(this, null);
    rayletClient = new MockRayletClient(this, rayConfig.numberExecThreadsForDevRuntime);
  }

  @Override
  public void shutdown() {
    rayletClient.destroy();
  }

  public MockObjectStore getObjectStore() {
    return store;
  }

  @Override
  public Worker getWorker() {
    return ((MockRayletClient) rayletClient).getCurrentWorker();
  }
}
