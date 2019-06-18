package org.ray.runtime;

import org.ray.runtime.config.RayConfig;

public class RayDevRuntime extends AbstractRayRuntime {

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

//  private MockObjectStore store;

  @Override
  public void start() {
    throw new UnsupportedOperationException();
//    store = new MockObjectStore(this);
//    objectStoreProxy = new ObjectStoreProxy(this, null);
//    rayletClient = new MockRayletClient(this, rayConfig.numberExecThreadsForDevRuntime);
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();
//    rayletClient.destroy();
  }

//  public MockObjectStore getObjectStore() {
//    return store;
//  }
//
//  @Override
//  public Worker getWorker() {
//    return ((MockRayletClient) rayletClient).getCurrentWorker();
//  }
}
