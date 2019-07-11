package org.ray.runtime;

import java.util.concurrent.atomic.AtomicInteger;
import org.ray.api.id.JobId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.objectstore.MockObjectInterface;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.MockRayletClient;

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

    objectInterface = new MockObjectInterface(workerContext);
    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }
    workerContext = new WorkerContext(rayConfig.workerMode,
        rayConfig.getJobId(), rayConfig.runMode);
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

  private JobId nextJobId() {
    return JobId.fromInt(jobCounter.getAndIncrement());
  }
}
