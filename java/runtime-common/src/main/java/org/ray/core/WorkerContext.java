package org.ray.core;

import org.ray.api.id.UniqueId;
import org.ray.core.model.RayParameters;
import org.ray.core.model.WorkerMode;
import org.ray.spi.model.TaskSpec;

public class WorkerContext {

  private static final ThreadLocal<WorkerContext> currentWorkerCtx =
      ThreadLocal.withInitial(() -> init(AbstractRayRuntime.getParams()));
  /**
   * id of worker.
   */
  public static UniqueId workerID = UniqueId.randomId();
  /**
   * current doing task.
   */
  private TaskSpec currentTask;
  /**
   * current app classloader.
   */
  private ClassLoader currentClassLoader;
  /**
   * how many puts done by current task.
   */
  private int currentTaskPutCount;
  /**
   * how many calls done by current task.
   */
  private int currentTaskCallCount;

  public static WorkerContext init(RayParameters params) {
    WorkerContext ctx = new WorkerContext();
    currentWorkerCtx.set(ctx);

    TaskSpec dummy = new TaskSpec();
    dummy.parentTaskId = UniqueId.NIL;
    if (params.worker_mode == WorkerMode.DRIVER) {
      dummy.taskId = UniqueId.randomId();
    } else {
      dummy.taskId = UniqueId.NIL;
    }
    dummy.actorId = UniqueId.NIL;
    dummy.driverId = params.driver_id;
    prepare(dummy, null);

    return ctx;
  }

  public static void prepare(TaskSpec task, ClassLoader classLoader) {
    WorkerContext wc = get();
    wc.currentTask = task;
    wc.currentTaskPutCount = 0;
    wc.currentTaskCallCount = 0;
    wc.currentClassLoader = classLoader;
  }

  public static WorkerContext get() {
    return currentWorkerCtx.get();
  }

  public static TaskSpec currentTask() {
    return get().currentTask;
  }

  public static int nextPutIndex() {
    return ++get().currentTaskPutCount;
  }

  public static int nextCallIndex() {
    return ++get().currentTaskCallCount;
  }

  public static UniqueId currentWorkerId() {
    return WorkerContext.workerID;
  }

  public static ClassLoader currentClassLoader() {
    return get().currentClassLoader;
  }
}
