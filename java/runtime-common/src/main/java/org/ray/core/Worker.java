package org.ray.core;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.UniqueID;
import org.ray.spi.LocalSchedulerLink;
import org.ray.spi.model.RayMethod;
import org.ray.spi.model.TaskSpec;
import org.ray.util.logger.RayLog;

/**
 * The worker, which pulls tasks from {@code org.ray.spi.LocalSchedulerProxy} and executes them
 * continuously.
 */
public class Worker {

  private final LocalSchedulerLink scheduler;
  private final LocalFunctionManager functions;

  public Worker(LocalSchedulerLink scheduler, LocalFunctionManager functions) {
    this.scheduler = scheduler;
    this.functions = functions;
  }

  public void loop() {
    while (true) {
      RayLog.core.info(Thread.currentThread().getName() + ":fetching new task...");
      TaskSpec task = scheduler.getTaskTodo();
      execute(task, functions);
    }
  }

  public static void execute(TaskSpec task, LocalFunctionManager funcs) {
    RayLog.core.info("Executing task {}", task.taskId);

    if (!task.actorId.isNil() || (task.createActorId != null && !task.createActorId.isNil())) {
      task.returnIds = ArrayUtils.subarray(task.returnIds, 0, task.returnIds.length - 1);
    }

    try {
      Pair<ClassLoader, RayMethod> pr = funcs
          .getMethod(task.driverId, task.actorId, task.functionId, task.args);
      WorkerContext.prepare(task, pr.getLeft());
      InvocationExecutor.execute(task, pr);
      RayLog.core.info("Finished executing task {}", task.taskId);
    } catch (Exception e) {
      RayLog.core.error("Failed to execute task " + task.taskId, e);
      BaseRayRuntime.getInstance().put(task.returnIds[0], e);
    }
  }

  public UniqueID getCurrentTaskId() {
    return WorkerContext.currentTask().taskId;
  }

  public UniqueID getCurrentTaskNextPutId() {
    return UniqueIdHelper.computePutId(
        WorkerContext.currentTask().taskId, WorkerContext.nextPutIndex());
  }

  public UniqueID[] getCurrentTaskReturnIDs() {
    return WorkerContext.currentTask().returnIds;
  }
}
