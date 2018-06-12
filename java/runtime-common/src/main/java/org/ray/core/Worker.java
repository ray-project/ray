package org.ray.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.RayActor;
import org.ray.api.RayList;
import org.ray.api.RayMap;
import org.ray.api.RayObject;
import org.ray.api.RayObjects;
import org.ray.api.UniqueID;
import org.ray.api.internal.Callable;
import org.ray.hook.runtime.MethodSwitcher;
import org.ray.spi.LocalSchedulerProxy;
import org.ray.spi.model.RayInvocation;
import org.ray.spi.model.RayMethod;
import org.ray.spi.model.TaskSpec;
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
      Pair<ClassLoader, RayMethod> pr = funcs.getMethod(task.driverId, task.functionId, task.args);
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

  public RayObjects rpc(UniqueID taskId, Callable funcRun, int returnCount, Object[] args) {
    byte[] fid = fidFromHook(funcRun);
    return submit(taskId, fid, returnCount, false, args);
  }

  public RayObjects rpc(UniqueID taskId, UniqueID functionId, Class<?> funcCls, Serializable lambda,
                        int returnCount, Object[] args) {
    byte[] fid = functionId.getBytes();

    Object[] ls = Arrays.copyOf(args, args.length + 2);
    ls[args.length] = funcCls.getName();
    ls[args.length + 1] = SerializationUtils.serialize(lambda);

    return submit(taskId, fid, returnCount, false, ls);
  }

  public RayObjects rpc(UniqueID taskId, RayActor<?> actor, Callable funcRun, int returnCount,
                        Object[] args) {
    byte[] fid = fidFromHook(funcRun);
    return actorTaskSubmit(taskId, fid, returnCount, false, args, actor);
  }

  public RayObjects rpc(UniqueID taskId, UniqueID functionId, RayActor<?> actor, Class<?> funcCls,
                        Serializable lambda, int returnCount, Object[] args) {
    byte[] fid = functionId.getBytes();

    Object[] ls = Arrays.copyOf(args, args.length + 2);
    ls[args.length] = funcCls.getName();
    ls[args.length + 1] = SerializationUtils.serialize(lambda);

    return actorTaskSubmit(taskId, fid, returnCount, false, ls, actor);
  }

  private byte[] fidFromHook(Callable funcRun) {
    MethodSwitcher.IsRemoteCall.set(true);
    try {
      funcRun.run();
    } catch (Throwable e) {
      RayLog.core.error(
          "make sure you are using code rewritten using the rewrite tool, see JarRewriter for"
              + " options", e);
      throw new RuntimeException("make sure you are using code rewritten using the rewrite tool,"
          + "see JarRewriter for options");
    }
    byte[] fid = MethodSwitcher.MethodId.get();//get the identity of function from hook
    MethodSwitcher.IsRemoteCall.set(false);
    return fid;
  }

  private RayObjects submit(UniqueID taskId,
                            byte[] fid,
                            int returnCount,
                            boolean multiReturn,
                            Object[] args) {
    if (taskId == null) {
      taskId = UniqueIdHelper.nextTaskId(-1);
    }
    if (args.length > 0 && args[0].getClass().equals(RayActor.class)) {
      return actorTaskSubmit(taskId, fid, returnCount, multiReturn, args, (RayActor<?>) args[0]);
    } else {
      return taskSubmit(taskId, fid, returnCount, multiReturn, args);
    }
  }

  private RayObjects actorTaskSubmit(UniqueID taskId,
                                     byte[] fid,
                                     int returnCount,
                                     boolean multiReturn,
                                     Object[] args,
                                     RayActor<?> actor) {
    RayInvocation ri = new RayInvocation(fid, args, actor);
    RayObjects returnObjs = scheduler.submit(taskId, ri, returnCount + 1, multiReturn);
    actor.setTaskCursor(returnObjs.pop().getId());
    return returnObjs;
  }

  private RayObjects taskSubmit(UniqueID taskId,
                                byte[] fid,
                                int returnCount,
                                boolean multiReturn,
                                Object[] args) {
    RayInvocation ri = new RayInvocation(fid, args);
    return scheduler.submit(taskId, ri, returnCount, multiReturn);
  }

  public RayObjects rpcCreateActor(UniqueID taskId, UniqueID createActorId, Callable funcRun,
                                   int returnCount,
                                   Object[] args) {
    byte[] fid = fidFromHook(funcRun);
    RayInvocation ri = new RayInvocation(fid, new Object[] {});
    return scheduler.submit(taskId, createActorId, ri, returnCount, false);
  }

  public RayObjects rpcCreateActor(UniqueID taskId, UniqueID createActorId, UniqueID functionId,
                                   Class<?> funcCls, Serializable lambda, int returnCount,
                                   Object[] args) {
    byte[] fid = functionId.getBytes();

    Object[] ls = Arrays.copyOf(args, args.length + 2);
    ls[args.length] = funcCls.getName();
    ls[args.length + 1] = SerializationUtils.serialize(lambda);

    RayInvocation ri = new RayInvocation(fid, ls);
    return scheduler.submit(taskId, createActorId, ri, returnCount, false);
  }

  public <R, RIDT> RayMap<RIDT, R> rpcWithReturnLabels(UniqueID taskId, Callable funcRun,
                                                       Collection<RIDT> returnids,
                                                       Object[] args) {
    byte[] fid = fidFromHook(funcRun);
    if (taskId == null) {
      taskId = UniqueIdHelper.nextTaskId(-1);
    }
    return scheduler.submit(taskId, new RayInvocation(fid, args), returnids);
  }

  public <R, RIDT> RayMap<RIDT, R> rpcWithReturnLabels(UniqueID taskId, Class<?> funcCls,
                                                       Serializable lambda,
                                                       Collection<RIDT> returnids,
                                                       Object[] args) {
    final byte[] fid = UniqueID.nil.getBytes();
    if (taskId == null) {
      taskId = UniqueIdHelper.nextTaskId(-1);
    }

    Object[] ls = Arrays.copyOf(args, args.length + 2);
    ls[args.length] = funcCls.getName();
    ls[args.length + 1] = SerializationUtils.serialize(lambda);

    return scheduler.submit(taskId, new RayInvocation(fid, ls), returnids);
  }

  public <R> RayList<R> rpcWithReturnIndices(UniqueID taskId, Callable funcRun,
                                             Integer returnCount,
                                             Object[] args) {
    byte[] fid = fidFromHook(funcRun);
    RayObjects objs = submit(taskId, fid, returnCount, true, args);
    RayList<R> rets = new RayList<>();
    for (RayObject obj : objs.getObjs()) {
      rets.add(obj);
    }
    return rets;
  }

  @SuppressWarnings("unchecked")
  public <R> RayList<R> rpcWithReturnIndices(UniqueID taskId, Class<?> funcCls,
                                             Serializable lambda, Integer returnCount,
                                             Object[] args) {
    byte[] fid = UniqueID.nil.getBytes();
    Object[] ls = Arrays.copyOf(args, args.length + 2);
    ls[args.length] = funcCls.getName();
    ls[args.length + 1] = SerializationUtils.serialize(lambda);

    RayObjects objs = submit(taskId, fid, returnCount, true, ls);

    RayList<R> rets = new RayList<>();
    for (RayObject obj : objs.getObjs()) {
      rets.add(obj);
    }
    return rets;
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
