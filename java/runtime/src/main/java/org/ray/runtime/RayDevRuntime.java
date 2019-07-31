package org.ray.runtime;

import java.util.concurrent.atomic.AtomicInteger;
import org.ray.api.id.JobId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.objectstore.MockObjectInterface;

public class RayDevRuntime extends AbstractRayRuntime {

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private AtomicInteger jobCounter = new AtomicInteger(0);

  MockObjectInterface objectInterface;
  MockTaskInterface taskInterface;

  @Override
  public void start() {
    // Reset library path at runtime.
    resetLibraryPath();

    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }
    objectInterface = new MockObjectInterface();
    taskInterface = new MockTaskInterface(this, objectInterface,
        rayConfig.numberExecThreadsForDevRuntime);
    objectInterface.addObjectPutCallback(taskInterface::onObjectPut);
    worker = new MockWorker(this);
    taskInterface.setCurrentWorker((MockWorker) worker);
  }

  @Override
  public void shutdown() {
    worker = null;
  }

  @Override
  public AbstractWorker getWorker() {
    return ((MockTaskInterface) worker.getTaskInterface()).getCurrentWorker();
  }

  private JobId nextJobId() {
    return JobId.fromInt(jobCounter.getAndIncrement());
  }
}
