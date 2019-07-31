package org.ray.runtime;

import java.util.List;
import org.ray.runtime.nativeTypes.NativeRayObject;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.MockRayletClient;

class MockWorker extends AbstractWorker {

  MockWorker(RayDevRuntime runtime) {
    super(runtime);
    this.objectStoreProxy = new ObjectStoreProxy(workerContext, runtime.objectInterface);
    this.workerContext = new MockWorkerContext(runtime.rayConfig.getJobId());
    this.taskInterface = runtime.taskInterface;
    this.rayletClient = new MockRayletClient();
  }

  @Override
  protected List<NativeRayObject> execute(List<String> rayFunctionInfo,
      List<NativeRayObject> argsBytes) {
    return super.execute(rayFunctionInfo, argsBytes);
  }
}
