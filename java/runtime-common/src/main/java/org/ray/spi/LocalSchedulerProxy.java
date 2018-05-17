package org.ray.spi;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.ray.api.RayMap;
import org.ray.api.RayObject;
import org.ray.api.RayObjects;
import org.ray.api.UniqueID;
import org.ray.core.ArgumentsBuilder;
import org.ray.core.UniqueIdHelper;
import org.ray.core.WorkerContext;
import org.ray.spi.model.RayInvocation;
import org.ray.spi.model.TaskSpec;
import org.ray.util.logger.RayLog;

/**
 * Local scheduler proxy, which provides a user-friendly facet on top of {code
 * org.ray.spi.LocalSchedulerLink}.
 */
@SuppressWarnings("rawtypes")
public class LocalSchedulerProxy {

  private final LocalSchedulerLink scheduler;

  public LocalSchedulerProxy(LocalSchedulerLink scheduler) {
    this.scheduler = scheduler;
  }

  public RayObjects submit(UniqueID taskId, RayInvocation invocation, int returnCount,
      boolean multiReturn) {
    UniqueID[] returnIds = buildReturnIds(taskId, returnCount, multiReturn);
    this.doSubmit(invocation, taskId, returnIds, UniqueID.nil);
    return new RayObjects(returnIds);
  }

  public RayObjects submit(UniqueID taskId, UniqueID createActorId, RayInvocation invocation,
      int returnCount, boolean multiReturn) {
    UniqueID[] returnIds = buildReturnIds(taskId, returnCount, multiReturn);
    this.doSubmit(invocation, taskId, returnIds, createActorId);
    return new RayObjects(returnIds);
  }

  public <R, RID> RayMap<RID, R> submit(UniqueID taskId, RayInvocation invocation,
      Collection<RID> userReturnIds) {
    UniqueID[] returnIds = buildReturnIds(taskId, userReturnIds.size(), true);

    RayMap<RID, R> ret = new RayMap<>();
    Map<RID, UniqueID> returnidmapArg = new HashMap<>();
    int index = 0;
    for (RID userReturnId : userReturnIds) {
      if (returnidmapArg.containsKey(userReturnId)) {
        RayLog.core.error("TaskId " + taskId + " userReturnId is duplicate " + userReturnId);
        continue;
      }
      returnidmapArg.put(userReturnId, returnIds[index]);
      ret.put(userReturnId, new RayObject<>(returnIds[index]));
      index++;
    }
    if (index < returnIds.length) {
      UniqueID[] newReturnIds = new UniqueID[index];
      System.arraycopy(returnIds, 0, newReturnIds, 0, index);
      returnIds = newReturnIds;
    }
    Object args[] = invocation.getArgs();
    Object[] newargs;
    if (args == null) {
      newargs = new Object[]{returnidmapArg};
    } else {
      newargs = new Object[args.length + 1];
      newargs[0] = returnidmapArg;
      System.arraycopy(args, 0, newargs, 1, args.length);
    }
    invocation.setArgs(newargs);
    this.doSubmit(invocation, taskId, returnIds, UniqueID.nil);
    return ret;
  }

  private void doSubmit(RayInvocation invocation, UniqueID taskId,
      UniqueID[] returnIds, UniqueID createActorId) {

    TaskSpec current = WorkerContext.currentTask();
    TaskSpec task = new TaskSpec();
    task.actorCounter = invocation.getActor().increaseTaskCounter();
    task.actorId = invocation.getActor().getId();
    task.createActorId = createActorId;

    task.args = ArgumentsBuilder.wrap(invocation);
    task.driverId = current.driverId;
    task.functionId = new UniqueID(invocation.getId());
    task.parentCounter = -1; // TODO: this field is not used in core or python logically yet
    task.parentTaskId = current.taskId;
    task.actorHandleId = invocation.getActor().getActorHandleId();
    task.taskId = taskId;
    task.returnIds = returnIds;
    task.cursorId = invocation.getActor() != null ? invocation.getActor().getTaskCursor() : null;

    //WorkerContext.onSubmitTask();
    RayLog.core.info(
        "Task " + taskId + " submitted, functionId = " + task.functionId + " actorId = "
            + task.actorId + ", driverId = " + task.driverId + ", return_ids = " + Arrays
            .toString(returnIds) + ", currentTask " + WorkerContext.currentTask().taskId
                + " cursorId = " + task.cursorId);
    scheduler.submitTask(task);
  }

  // build Object IDs of return values.
  private UniqueID[] buildReturnIds(UniqueID taskId, int returnCount, boolean multiReturn) {
    UniqueID[] returnIds = new UniqueID[returnCount];
    for (int k = 0; k < returnCount; k++) {
      returnIds[k] = UniqueIdHelper.taskComputeReturnId(taskId, k, multiReturn);
    }
    return returnIds;
  }

  public TaskSpec getTask() {
    TaskSpec ts = scheduler.getTaskTodo();
    RayLog.core.info("Task " + ts.taskId.toString() + " received");
    return ts;
  }

  public void markTaskPutDependency(UniqueID taskId, UniqueID objectId) {
    scheduler.markTaskPutDependency(taskId, objectId);
  }

  public void reconstructObject(UniqueID objectId) {
    scheduler.reconstructObject(objectId);
  }

  public void notifyUnblocked() {
    scheduler.notifyUnblocked();
  }
}
