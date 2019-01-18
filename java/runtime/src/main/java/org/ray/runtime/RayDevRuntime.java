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
    rayletClient = new MockRayletClient(this, store);
  }

  @Override
  public void shutdown() {
    // nothing to do
  }

  public MockObjectStore getObjectStore() {
    return store;
  }
}
