package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.WorkerMode;

public class RayDevRuntime extends AbstractRayRuntime {

  private final List<Thread> workerThreads = new ArrayList<>();
  private final List<Worker> workers = new ArrayList<>();

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  @Override
  public void start() {
    Preconditions.checkState(rayConfig.get().workerMode == WorkerMode.DRIVER);
    super.start();

    worker.set(new Worker(WorkerMode.DRIVER, this, functionManager,
        rayConfig.get().objectStoreSocketName, rayConfig.get().rayletSocketName,
        rayConfig.get().jobId));

//    System.setProperty("ray.worker.mode", "WORKER");
    for (int i = 0; i < rayConfig.get().numberExecThreadsForDevRuntime; i++) {
      Thread thread = new Thread(() -> {
//        rayConfig.set(RayConfig.create());
        Worker newWorker = new Worker(WorkerMode.WORKER, this, functionManager,
            rayConfig.get().objectStoreSocketName, rayConfig.get().rayletSocketName,
            UniqueId.NIL);
        worker.set(newWorker);
        workers.add(newWorker);
        newWorker.loop();
      });
      thread.start();
      workerThreads.add(thread);
    }
  }

  @Override
  public void shutdown() {
    for (Worker worker : workers) {
      worker.stop();
    }
    for (Thread thread : workerThreads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
