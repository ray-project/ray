package org.ray.runtime;

import java.util.concurrent.atomic.AtomicInteger;
import org.ray.api.id.JobId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.objectstore.MockObjectInterface;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.MockRayletClient;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class RayDevRuntime extends AbstractRayRuntime {

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private MockObjectInterface objectInterface;

  private AtomicInteger jobCounter = new AtomicInteger(0);

  @Override
  public void start() {
    // Reset library path at runtime.
    resetLibraryPath();

    objectInterface = new MockObjectInterface();
    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }
    throw new NotImplementedException();
  }

  @Override
  public void shutdown() {
    //worker.destroy();
  }

  @Override
  public Worker getWorker() {
//    return ((MockRayletClient) rayletClient).getCurrentWorker();
    throw new NotImplementedException();
  }

  private JobId nextJobId() {
    return JobId.fromInt(jobCounter.getAndIncrement());
  }
}
