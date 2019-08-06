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
    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }
    objectInterface = new MockObjectInterface();
    taskInterface = new MockTaskInterface(this, objectInterface,
        rayConfig.numberExecThreadsForDevRuntime);
    objectInterface.addObjectPutCallback(taskInterface::onObjectPut);
    worker = new MockWorker(this);
  }

  @Override
  public void shutdown() {
    worker = null;
  }

  @Override
  public AbstractWorker getWorker() {
    AbstractWorker result = ((MockTaskInterface) worker.getTaskInterface()).getCurrentWorker();
    if (result == null) {
      // This is a workaround to support multi-threading in driver when running in single process mode.
      result = worker;
    }
    return result;
  }

  private JobId nextJobId() {
    return JobId.fromInt(jobCounter.getAndIncrement());
  }
}
