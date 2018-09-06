package org.ray.runtime;

import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.config.RayParameters;
import org.ray.runtime.functionmanager.NopRemoteFunctionManager;
import org.ray.runtime.config.PathConfig;
import org.ray.runtime.functionmanager.RemoteFunctionManager;
import org.ray.runtime.raylet.MockLocalScheduler;
import org.ray.runtime.objectstore.MockObjectStore;

public class RayDevRuntime extends AbstractRayRuntime {

  @Override
  public void start(RayParameters params) {
    PathConfig pathConfig = new PathConfig(configReader);
    RemoteFunctionManager rfm = new NopRemoteFunctionManager(params.driver_id);
    MockObjectStore store = new MockObjectStore();
    MockLocalScheduler scheduler = new MockLocalScheduler(this, store);
    init(scheduler, store, rfm, pathConfig);
    scheduler.setLocalFunctionManager(this.functions);
  }

  @Override
  public void shutdown() {
    // nothing to do
  }
}
