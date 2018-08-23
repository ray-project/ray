package org.ray.spi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.ray.api.RayObject;
import org.ray.api.RayObjects;
import org.ray.api.UniqueID;
import org.ray.api.WaitResult;
import org.ray.core.ArgumentsBuilder;
import org.ray.core.UniqueIdHelper;
import org.ray.core.WorkerContext;
import org.ray.spi.model.RayInvocation;
import org.ray.spi.model.TaskSpec;
import org.ray.util.ResourceUtil;
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

  // build Object IDs of return values.
  private UniqueID[] buildReturnIds(UniqueID taskId, int returnCount, boolean multiReturn) {
    UniqueID[] returnIds = new UniqueID[returnCount];
    for (int k = 0; k < returnCount; k++) {
      returnIds[k] = UniqueIdHelper.taskComputeReturnId(taskId, k, multiReturn);
    }
    return returnIds;
  }

  private void doSubmit(RayInvocation invocation, UniqueID taskId,
                        UniqueID[] returnIds, UniqueID createActorId) {

    final TaskSpec current = WorkerContext.currentTask();
    TaskSpec task = new TaskSpec();
    task.actorCounter = invocation.getActor().increaseTaskCounter();
    task.actorId = invocation.getActor().getId();
    task.createActorId = createActorId;

    task.args = ArgumentsBuilder.wrap(invocation);
    task.driverId = current.driverId;
    task.functionId = invocation.getId();
    task.parentCounter = -1; // TODO: this field is not used in core or python logically yet
    task.parentTaskId = current.taskId;
    task.actorHandleId = invocation.getActor().getActorHandleId();
    task.taskId = taskId;
    task.returnIds = returnIds;
    task.cursorId = invocation.getActor() != null ? invocation.getActor().getTaskCursor() : null;
    task.resources = ResourceUtil
                         .getResourcesMapFromArray(invocation.getRemoteAnnotation().resources());

    //WorkerContext.onSubmitTask();
    RayLog.core.info(
        "Task " + taskId + " submitted, functionId = " + task.functionId + " actorId = "
            + task.actorId + ", driverId = " + task.driverId + ", return_ids = " + Arrays
            .toString(returnIds) + ", currentTask " + WorkerContext.currentTask().taskId
            + " cursorId = " + task.cursorId);
    scheduler.submitTask(task);
  }

  public TaskSpec getTask() {
    TaskSpec ts = scheduler.getTaskTodo();
    RayLog.core.info("Task " + ts.taskId.toString() + " received");
    return ts;
  }

  public void markTaskPutDependency(UniqueID taskId, UniqueID objectId) {
    scheduler.markTaskPutDependency(taskId, objectId);
  }

  public void reconstructObject(UniqueID objectId, boolean fetchOnly) {
    scheduler.reconstructObject(objectId, fetchOnly);
  }

  public void reconstructObjects(List<UniqueID> objectIds, boolean fetchOnly) {
    scheduler.reconstructObjects(objectIds, fetchOnly);
  }

  public void notifyUnblocked() {
    scheduler.notifyUnblocked();
  }

  private static byte[][] getIdBytes(List<UniqueID> objectIds) {
    int size = objectIds.size();
    byte[][] ids = new byte[size][];
    for (int i = 0; i < size; i++) {
      ids[i] = objectIds.get(i).getBytes();
    }
    return ids;
  }

  public <T> WaitResult<T> wait(List<RayObject<T>> waitfor, int numReturns, int timeout) {
    List<UniqueID> ids = new ArrayList<>();
    for (RayObject<T> obj : waitfor) {
      ids.add(obj.getId());
    }
    List<byte[]> readys = scheduler.wait(getIdBytes(ids), timeout, numReturns);

    List<RayObject<T>> readyObjs = new ArrayList<>();
    List<RayObject<T>> remainObjs = new ArrayList<>();
    for (RayObject<T> obj : waitfor) {
      if (readys.contains(obj.getId().getBytes())) {
        readyObjs.add(obj);
      } else {
        remainObjs.add(obj);
      }
    }

    return new WaitResult<>(readyObjs, remainObjs);
  }
}
