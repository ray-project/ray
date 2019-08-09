package org.ray.runtime;

import java.util.concurrent.atomic.AtomicInteger;
import org.ray.api.id.JobId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.context.LocalModeWorkerContext;
import org.ray.runtime.object.LocalModeObjectStore;
import org.ray.runtime.raylet.LocalModeRayletClient;
import org.ray.runtime.task.LocalModeTaskSubmitter;
import org.ray.runtime.task.TaskExecutor;

public class RayDevRuntime extends AbstractRayRuntime {

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private AtomicInteger jobCounter = new AtomicInteger(0);

  @Override
  public void start() {
    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }
    taskExecutor = new TaskExecutor(this);
    workerContext = new LocalModeWorkerContext(rayConfig.getJobId());
    objectStore = new LocalModeObjectStore(workerContext);
    taskSubmitter = new LocalModeTaskSubmitter(this, (LocalModeObjectStore) objectStore,
        rayConfig.numberExecThreadsForDevRuntime);
    ((LocalModeObjectStore) objectStore).addObjectPutCallback(
        objectId -> ((LocalModeTaskSubmitter) taskSubmitter).onObjectPut(objectId));
    rayletClient = new LocalModeRayletClient();
  }

  @Override
  public void shutdown() {
    taskExecutor = null;
  }

  private JobId nextJobId() {
    return JobId.fromInt(jobCounter.getAndIncrement());
  }
}
