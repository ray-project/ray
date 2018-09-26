package org.ray.runtime;

import org.ray.runtime.config.PathConfig;
import org.ray.runtime.config.RayParameters;
import org.ray.runtime.objectstore.MockObjectStore;
import org.ray.runtime.raylet.MockRayletClient;

public class RayDevRuntime extends AbstractRayRuntime {

  @Override
  public void start(RayParameters params) {
    PathConfig pathConfig = new PathConfig(configReader);
    MockObjectStore store = new MockObjectStore();
    MockRayletClient scheduler = new MockRayletClient(this, store);
    init(scheduler, store, pathConfig);
  }

  @Override
  public void shutdown() {
    // nothing to do
  }
}
