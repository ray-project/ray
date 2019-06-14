package org.ray.runtime;

import java.util.List;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.RayFunction;

public class CoreWorkerProxy {
  private final long nativeCoreWorker;

  public CoreWorkerProxy(String storeSocket, String rayletSocket, UniqueId driverId) {
    nativeCoreWorker = createCoreWorker(storeSocket, rayletSocket, driverId.getBytes());
  }

  protected void runTaskCallback(List<String> rayFunction, List<byte[]> args, byte[] taskId,
                         int num_returns) {
  }

  protected void runTask(RayFunction rayFunction, List<byte[]> args, TaskId taskId,
                         int num_returns) {
  }

  public void run() {
    runCoreWorker(nativeCoreWorker, this);
  }

  private static native long createCoreWorker(String storeSocket, String rayletSocket,
                                              byte[] driverId);

  private static native void runCoreWorker(long nativeCoreWorker,
                                             CoreWorkerProxy coreWorkerProxy);
}
