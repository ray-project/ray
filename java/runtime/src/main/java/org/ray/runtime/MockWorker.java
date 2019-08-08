package org.ray.runtime;

import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.MockRayletClient;

class MockWorker extends AbstractWorker {

  MockWorker(RayDevRuntime runtime) {
    super(runtime);
    this.workerContext = new MockWorkerContext(runtime.rayConfig.getJobId());
    this.objectStoreProxy = new ObjectStoreProxy(workerContext, runtime.objectInterface);
    this.taskInterface = runtime.taskInterface;
    this.rayletClient = new MockRayletClient();
  }

}
