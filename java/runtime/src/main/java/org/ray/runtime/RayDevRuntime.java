package org.ray.runtime;

import java.util.concurrent.atomic.AtomicInteger;
import org.ray.api.id.JobId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.object.LocalModeObjectStore;
import org.ray.runtime.task.LocalModeTaskExecutor;
import org.ray.runtime.task.LocalModeTaskSubmitter;
import org.ray.runtime.task.TaskExecutor;

public class RayDevRuntime extends AbstractRayRuntime {

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private AtomicInteger jobCounter = new AtomicInteger(0);

  private LocalModeObjectStore objectStore;
  private LocalModeTaskSubmitter taskSubmitter;

  @Override
  public void start() {
    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }
    objectStore = new LocalModeObjectStore();
    taskSubmitter = new LocalModeTaskSubmitter(this, objectStore,
        rayConfig.numberExecThreadsForDevRuntime);
    objectStore.addObjectPutCallback(taskSubmitter::onObjectPut);
    taskExecutor = new LocalModeTaskExecutor(this);
  }

  @Override
  public void shutdown() {
    taskExecutor = null;
  }

  @Override
  public TaskExecutor getTaskExecutor() {
    TaskExecutor result = ((LocalModeTaskSubmitter) taskExecutor.getTaskSubmitter()).getCurrentTaskExecutor();
    if (result == null) {
      // This is a workaround to support multi-threading in driver when running in single process mode.
      result = taskExecutor;
    }
    return result;
  }

  private JobId nextJobId() {
    return JobId.fromInt(jobCounter.getAndIncrement());
  }

  public LocalModeObjectStore getObjectStore() {
    return objectStore;
  }

  public LocalModeTaskSubmitter getTaskSubmitter() {
    return taskSubmitter;
  }

}
