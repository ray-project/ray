package org.ray.spi;

import java.util.ArrayList;
import java.util.List;
import org.ray.api.RayObject;
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

  public RayObject submit(UniqueID taskId, RayInvocation invocation) {
    UniqueID[] returnIds = genReturnIds(taskId, 1);
    this.doSubmit(invocation, taskId, returnIds, UniqueID.NIL);
    return new RayObject(returnIds[0]);
  }

  public RayObject submitActorTask(UniqueID taskId, RayInvocation invocation) {
    // add one for the dummy return ID
    UniqueID[] returnIds = genReturnIds(taskId, 2);
    this.doSubmit(invocation, taskId, returnIds, UniqueID.NIL);
    return new RayObject(returnIds[0]);
  }

  public RayObject submitActorCreationTask(UniqueID taskId, UniqueID createActorId,
      RayInvocation invocation) {
    UniqueID[] returnIds = genReturnIds(taskId, 1);
    this.doSubmit(invocation, taskId, returnIds, createActorId);
    return new RayObject(returnIds[0]);
  }

  // generate the return ids of a task.
  private UniqueID[] genReturnIds(UniqueID taskId, int numReturns) {
    UniqueID[] ret = new UniqueID[numReturns];
    for (int i = 0; i < numReturns; i++) {
      ret[i] = UniqueIdHelper.computeReturnId(taskId, i + 1);
    }
    return ret;
  }

  private void doSubmit(RayInvocation invocation, UniqueID taskId, UniqueID[] returnIds,
      UniqueID createActorId) {

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
    task.resources = ResourceUtil.getResourcesMapFromArray(
        invocation.getRemoteAnnotation().resources());

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

  public UniqueID generateTaskId(UniqueID driverId, UniqueID parentTaskId, int taskIndex) {
    return scheduler.generateTaskId(driverId, parentTaskId, taskIndex);
  }
}
