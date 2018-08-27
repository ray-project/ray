package org.ray.core.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.annotation.RayRemote;
import org.ray.api.UniqueID;
import org.ray.api.function.RayFunc2;
import org.ray.core.RayRuntime;
import org.ray.core.UniqueIdHelper;
import org.ray.core.WorkerContext;
import org.ray.core.model.RayParameters;
import org.ray.spi.NopRemoteFunctionManager;
import org.ray.spi.PathConfig;
import org.ray.spi.RemoteFunctionManager;
import org.ray.spi.impl.MockLocalScheduler;
import org.ray.spi.impl.MockObjectStore;
import org.ray.util.exception.TaskExecutionException;
import org.ray.util.logger.RayLog;

public class RayDevRuntime extends RayRuntime {

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
  public void cleanUp() {
    // nothing to do
  }
}
