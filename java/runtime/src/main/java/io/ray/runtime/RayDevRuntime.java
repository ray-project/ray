package io.ray.runtime;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActor;
import io.ray.api.id.JobId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.context.LocalModeWorkerContext;
import io.ray.runtime.object.LocalModeObjectStore;
import io.ray.runtime.task.LocalModeTaskExecutor;
import io.ray.runtime.task.LocalModeTaskSubmitter;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayDevRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayDevRuntime.class);

  private AtomicInteger jobCounter = new AtomicInteger(0);

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  @Override
  public void start() {
    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }
    taskExecutor = new LocalModeTaskExecutor(this);
    workerContext = new LocalModeWorkerContext(rayConfig.getJobId());
    objectStore = new LocalModeObjectStore(workerContext);
    taskSubmitter = new LocalModeTaskSubmitter(this, taskExecutor,
        (LocalModeObjectStore) objectStore);
    ((LocalModeObjectStore) objectStore).addObjectPutCallback(
        objectId -> {
          if (taskSubmitter != null) {
            ((LocalModeTaskSubmitter) taskSubmitter).onObjectPut(objectId);
          }
        });
  }

  @Override
  public void run() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown() {
    if (taskSubmitter != null) {
      ((LocalModeTaskSubmitter) taskSubmitter).shutdown();
      taskSubmitter = null;
    }
    taskExecutor = null;
    RayConfig.reset();
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    LOGGER.error("Not implemented under SINGLE_PROCESS mode.");
  }

  @Override
  public void killActor(BaseActor actor, boolean noReconstruction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getAsyncContext() {
    return null;
  }

  @Override
  public void setAsyncContext(Object asyncContext) {
    Preconditions.checkArgument(asyncContext == null);
    super.setAsyncContext(asyncContext);
  }

  private JobId nextJobId() {
    return JobId.fromInt(jobCounter.getAndIncrement());
  }
}
