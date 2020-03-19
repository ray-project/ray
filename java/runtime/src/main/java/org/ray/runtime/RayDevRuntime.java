package org.ray.runtime;

import java.util.concurrent.atomic.AtomicInteger;
import org.ray.api.BaseActor;
import org.ray.api.id.JobId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.context.LocalModeWorkerContext;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.object.LocalModeObjectStore;
import org.ray.runtime.task.LocalModeTaskExecutor;
import org.ray.runtime.task.LocalModeTaskSubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayDevRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayDevRuntime.class);

  private AtomicInteger jobCounter = new AtomicInteger(0);

  public RayDevRuntime(RayConfig rayConfig, FunctionManager functionManager) {
    super(rayConfig, functionManager);
    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }
    taskExecutor = new LocalModeTaskExecutor(this);
    workerContext = new LocalModeWorkerContext(rayConfig.getJobId());
    objectStore = new LocalModeObjectStore(workerContext);
    taskSubmitter = new LocalModeTaskSubmitter(this, (LocalModeObjectStore) objectStore,
        rayConfig.numberExecThreadsForDevRuntime);
    ((LocalModeObjectStore) objectStore).addObjectPutCallback(
        objectId -> ((LocalModeTaskSubmitter) taskSubmitter).onObjectPut(objectId));
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
  }

  private JobId nextJobId() {
    return JobId.fromInt(jobCounter.getAndIncrement());
  }
}
