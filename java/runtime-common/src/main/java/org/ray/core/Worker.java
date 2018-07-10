package org.ray.core;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.RayActor;
import org.ray.api.RayList;
import org.ray.api.RayMap;
import org.ray.api.RayObject;
import org.ray.api.RayObjects;
import org.ray.api.UniqueID;
import org.ray.api.internal.RayFunc;
import org.ray.spi.LocalSchedulerProxy;
import org.ray.spi.model.RayInvocation;
import org.ray.spi.model.RayMethod;
import org.ray.spi.model.TaskSpec;
import org.ray.util.LambdaUtils;
import org.ray.util.MethodId;
import org.ray.util.exception.TaskExecutionException;
import org.ray.util.logger.RayLog;

/**
 * The worker, which pulls tasks from {@code org.ray.spi.LocalSchedulerProxy} and executes them
 * continuously.
 */
public class Worker {

  private final LocalSchedulerProxy scheduler;
  private final LocalFunctionManager functions;

  public Worker(LocalSchedulerProxy scheduler, LocalFunctionManager functions) {
    this.scheduler = scheduler;
    this.functions = functions;
  }

  public void loop() {
    while (true) {
      RayLog.core.info(Thread.currentThread().getName() + ":fetching new task...");
      TaskSpec task = scheduler.getTask();
      execute(task, functions);
    }
  }

  public static void execute(TaskSpec task, LocalFunctionManager funcs) {
    RayLog.core.info("Task " + task.taskId + " start execute");
    Throwable ex = null;

    if (!task.actorId.isNil() || (task.createActorId != null && !task.createActorId.isNil())) {
      task.returnIds = ArrayUtils.subarray(task.returnIds, 0, task.returnIds.length - 1);
    }

    try {
      Pair<ClassLoader, RayMethod> pr = funcs
          .getMethod(task.driverId, task.actorId, task.functionId, task.args);
      WorkerContext.prepare(task, pr.getLeft());
      InvocationExecutor.execute(task, pr);
    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
      RayLog.core.error("task execution failed for " + task.taskId, e);
      ex = new TaskExecutionException("task execution failed for " + task.taskId, e);
    } catch (Throwable e) {
      RayLog.core.error("catch Throwable when execute for " + task.taskId, e);
      ex = e;
    }

    if (ex != null) {
      for (int k = 0; k < task.returnIds.length; k++) {
        RayRuntime.getInstance().putRaw(task.returnIds[k], ex);
      }
    }

  }


  private RayObjects taskSubmit(UniqueID taskId,
      MethodId methodId,
      int returnCount,
      boolean multiReturn,
      Object[] args) {
    RayInvocation ri = createRemoteInvocation(methodId, args, RayActor.nil);
    return scheduler.submit(taskId, ri, returnCount, multiReturn);
  }

  private RayObjects actorTaskSubmit(UniqueID taskId,
      MethodId methodId,
      int returnCount,
      boolean multiReturn,
      Object[] args,
      RayActor<?> actor) {
    RayInvocation ri = createRemoteInvocation(methodId, args, actor);
    RayObjects returnObjs = scheduler.submit(taskId, ri, returnCount + 1, multiReturn);
    actor.setTaskCursor(returnObjs.pop().getId());
    return returnObjs;
  }

  private RayObjects submit(UniqueID taskId,
      MethodId methodId,
      int returnCount,
      boolean multiReturn,
      Object[] args) {
    if (taskId == null) {
      taskId = UniqueIdHelper.nextTaskId(-1);
    }
    if (args.length > 0 && args[0].getClass().equals(RayActor.class)) {
      return actorTaskSubmit(taskId, methodId, returnCount, multiReturn, args,
          (RayActor<?>) args[0]);
    } else {
      return taskSubmit(taskId, methodId, returnCount, multiReturn, args);
    }
  }


  public RayObjects rpc(UniqueID taskId, Class<?> funcCls, RayFunc lambda,
      int returnCount, Object[] args) {
    MethodId mid = methodIdOf(lambda);
    return submit(taskId, mid, returnCount, false, args);
  }

  public RayObjects rpcCreateActor(UniqueID taskId, UniqueID createActorId,
      Class<?> funcCls, RayFunc lambda, int returnCount, Object[] args) {
    Preconditions.checkNotNull(taskId);
    MethodId mid = methodIdOf(lambda);
    RayInvocation ri = createRemoteInvocation(mid, args, RayActor.nil);
    return scheduler.submit(taskId, createActorId, ri, returnCount, false);
  }

  public <R, RIDT> RayMap<RIDT, R> rpcWithReturnLabels(UniqueID taskId, Class<?> funcCls,
      RayFunc lambda, Collection<RIDT> returnids,
      Object[] args) {
    MethodId mid = methodIdOf(lambda);
    if (taskId == null) {
      taskId = UniqueIdHelper.nextTaskId(-1);
    }
    RayInvocation ri = createRemoteInvocation(mid, args, RayActor.nil);
    return scheduler.submit(taskId, ri, returnids);
  }

  public <R> RayList<R> rpcWithReturnIndices(UniqueID taskId, Class<?> funcCls,
      RayFunc lambda, Integer returnCount,
      Object[] args) {
    MethodId mid = methodIdOf(lambda);
    RayObjects objs = submit(taskId, mid, returnCount, true, args);
    RayList<R> rets = new RayList<>();
    for (RayObject obj : objs.getObjs()) {
      rets.add(obj);
    }
    return rets;
  }

  private RayInvocation createRemoteInvocation(MethodId methodId, Object[] args,
      RayActor<?> actor) {
    UniqueID driverId = WorkerContext.currentTask().driverId;

    Object[] ls = Arrays.copyOf(args, args.length + 1);
    ls[args.length] = methodId.className;

    RayMethod method = functions
        .getMethod(driverId, actor.getId(), new UniqueID(methodId.getSha1Hash()),
            methodId.className).getRight();

    RayInvocation ri = new RayInvocation(methodId.className, method.getFuncId(),
        ls, method.remoteAnnotation, actor);
    return ri;
  }

  private MethodId methodIdOf(RayFunc serialLambda) {
    MethodId mid = MethodId.fromSerializedLambda(serialLambda);
    return mid;
  }

  public UniqueID getCurrentTaskId() {
    return WorkerContext.currentTask().taskId;
  }

  public UniqueID getCurrentTaskNextPutId() {
    return UniqueIdHelper.taskComputePutId(
        WorkerContext.currentTask().taskId, WorkerContext.nextPutIndex());
  }

  public UniqueID[] getCurrentTaskReturnIDs() {
    return WorkerContext.currentTask().returnIds;
  }
}