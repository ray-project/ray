package org.ray.runtime.task;

import org.ray.runtime.context.LocalModeWorkerContext;
import org.ray.runtime.RayDevRuntime;
import org.ray.runtime.object.ObjectStoreProxy;
import org.ray.runtime.raylet.LocalModeRayletClient;

public class LocalModeTaskExecutor extends TaskExecutor {

  public LocalModeTaskExecutor(RayDevRuntime runtime) {
    super(runtime);
    this.workerContext = new LocalModeWorkerContext(runtime.getRayConfig().getJobId());
    this.objectStoreProxy = new ObjectStoreProxy(workerContext, runtime.getObjectStore());
    this.taskSubmitter = runtime.getTaskSubmitter();
    this.rayletClient = new LocalModeRayletClient();
  }

}
