package org.ray.core.impl;

import org.ray.core.AbstractRayRuntime;
import org.ray.core.model.RayParameters;
import org.ray.spi.NopRemoteFunctionManager;
import org.ray.spi.PathConfig;
import org.ray.spi.RemoteFunctionManager;
import org.ray.spi.impl.MockLocalScheduler;
import org.ray.spi.impl.MockObjectStore;

public class RayDevRuntime extends AbstractRayRuntime {

  @Override
  public void start(RayParameters params) {
    PathConfig pathConfig = new PathConfig(configReader);
    RemoteFunctionManager rfm = new NopRemoteFunctionManager(params.driver_id);
    MockObjectStore store = new MockObjectStore();
    MockLocalScheduler scheduler = new MockLocalScheduler(store);
    init(scheduler, store, rfm, pathConfig);
    scheduler.setLocalFunctionManager(this.functions);
  }

  @Override
  public void shutdown() {
    // nothing to do
  }
}
