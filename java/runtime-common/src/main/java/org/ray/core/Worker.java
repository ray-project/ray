package org.ray.core;

import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.UniqueID;
import org.ray.api.function.RayFunc;
import org.ray.api.function.RayFunc2;
import org.ray.spi.LocalSchedulerLink;
import org.ray.spi.model.RayMethod;
import org.ray.spi.model.TaskSpec;
import org.ray.util.MethodId;
import org.ray.util.ResourceUtil;
import org.ray.util.exception.TaskExecutionException;
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
      RayRuntime.getInstance().putRaw(task.returnIds[0], e);
    }
  }

  /**
   * generate the return ids of a task.
   */
  private UniqueID[] genReturnIds(UniqueID taskId, int numReturns) {
    UniqueID[] ret = new UniqueID[numReturns];
    for (int i = 0; i < numReturns; i++) {
      ret[i] = UniqueIdHelper.computeReturnId(taskId, i + 1);
    }
    return ret;
  }

  private TaskSpec createTaskSpec(RayFunc func, RayActor actor, Object[] args, Class actorClassForCreation) {
    final TaskSpec current = WorkerContext.currentTask();
    UniqueID taskId = scheduler.generateTaskId(current.driverId,
          current.taskId,
          WorkerContext.nextCallIndex());
    int numReturns = actor.getId().isNil() ? 1 : 2;
    UniqueID[] returnIds = genReturnIds(taskId, numReturns);

    UniqueID actorCreationId = UniqueID.NIL;
    if (actorClassForCreation != null) {
      args = new Object[] {returnIds[0], actorClassForCreation.getName()};
      actorCreationId = returnIds[0];
    }

    MethodId methodId = methodIdOf(func);

    args = Arrays.copyOf(args, args.length + 1);
    args[args.length - 1] = methodId.className;

    RayMethod rayMethod = functions.getMethod(
        current.driverId, actor.getId(), new UniqueID(methodId.getSha1Hash()), methodId.className
    ).getRight();
    UniqueID funcId = rayMethod.getFuncId();

    return new TaskSpec(
        current.driverId,
        taskId,
        current.taskId,
        -1,
        actor.getId(),
        actor.getTaskCounter(),
        funcId,
        ArgumentsBuilder.wrap(args),
        returnIds,
        actor.getHandleId(),
        actorCreationId,
        ResourceUtil.getResourcesMapFromArray(rayMethod.remoteAnnotation.resources()),
        actor.getLastTaskId()
    );
  }

  public RayObject submit(RayFunc func, RayActor actor, Object[] args) {
    TaskSpec spec = createTaskSpec(func, actor, args, null);
    if (!actor.getId().isNil()) {
      actor.onSubmittingTask(spec.returnIds[1]);
    }
    scheduler.submitTask(spec);
    return new RayObjectImpl(spec.returnIds[0]);
  }

  public RayActor submitActorCreationTask(RayFunc2<UniqueID, String, Object> func, Class actorClass) {
    TaskSpec spec = createTaskSpec(func, RayActorImpl.NIL, null, actorClass);
    RayActorImpl actor = new RayActorImpl(spec.returnIds[0]);
    actor.onSubmittingTask(spec.returnIds[0]);
    scheduler.submitTask(spec);
    return actor;
  }

  private MethodId methodIdOf(RayFunc serialLambda) {
    return MethodId.fromSerializedLambda(serialLambda);
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
