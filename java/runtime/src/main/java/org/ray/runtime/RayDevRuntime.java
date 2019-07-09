package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RayConfig;

public class RayDevRuntime extends AbstractRayRuntime {

  private final Queue<Thread> workerThreads = new ConcurrentLinkedDeque<>();
  private final Queue<Worker> workers = new ConcurrentLinkedDeque<>();
  private CountDownLatch countDownLatch;

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
    Preconditions.checkState(countDownLatch == null);
    countDownLatch = new CountDownLatch(rayConfig.get().numberExecThreadsForDevRuntime);
    for (int i = 0; i < rayConfig.get().numberExecThreadsForDevRuntime; i++) {
      Thread thread = new Thread(() -> {
//        rayConfig.set(RayConfig.create());
        Worker newWorker = new Worker(WorkerMode.WORKER, this, functionManager,
            rayConfig.get().objectStoreSocketName, rayConfig.get().rayletSocketName,
            UniqueId.NIL);
        worker.set(newWorker);
        workers.add(newWorker);
        countDownLatch.countDown();
        newWorker.loop();
      });
      thread.start();
      workerThreads.add(thread);
    }
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
    while (!workers.isEmpty()) {
      Worker worker = workers.poll();
      worker.stop();
    }
    while (!workerThreads.isEmpty()) {
      try {
        workerThreads.poll().join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    countDownLatch = null;
  }
}
