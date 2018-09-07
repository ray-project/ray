package org.ray.runtime;

import org.ray.runtime.config.RayParameters;
import org.ray.runtime.functionmanager.NopRemoteFunctionManager;
import org.ray.runtime.config.PathConfig;
import org.ray.runtime.functionmanager.RemoteFunctionManager;
import org.ray.runtime.raylet.MockRayletClient;
import org.ray.runtime.objectstore.MockObjectStore;

public class RayDevRuntime extends AbstractRayRuntime {

  @Override
  public void start(RayParameters params) {
    PathConfig pathConfig = new PathConfig(configReader);
    RemoteFunctionManager rfm = new NopRemoteFunctionManager(params.driver_id);
    MockObjectStore store = new MockObjectStore();
    MockRayletClient scheduler = new MockRayletClient(this, store);
    init(scheduler, store, rfm, pathConfig);
    scheduler.setLocalFunctionManager(this.functions);
  }

  @Override
  public void shutdown() {
    // nothing to do
  }
}
