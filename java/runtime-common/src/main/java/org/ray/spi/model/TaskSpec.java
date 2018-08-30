package org.ray.spi.model;

import java.util.Arrays;
import java.util.Map;
import org.ray.api.id.UniqueId;
import org.ray.util.ResourceUtil;

/**
 * Represents necessary information of a task for scheduling and executing.
 */
public class TaskSpec {

  // ID of the driver that created this task.
  public UniqueId driverId;

  // Task ID of the task.
  public UniqueId taskId;

  // Task ID of the parent task.
  public UniqueId parentTaskId;

  // A count of the number of tasks submitted by the parent task before this one.
  public int parentCounter;

  // Actor ID of the task. This is the actor that this task is executed on
  // or NIL_ACTOR_ID if the task is just a normal task.
  public UniqueId actorId;

  // Number of tasks that have been submitted to this actor so far.
  public int actorCounter;

  // Function ID of the task.
  public UniqueId functionId;

  // Task arguments.
  public FunctionArg[] args;

  // return ids
  public UniqueId[] returnIds;

  // ID per actor client for session consistency
  public UniqueId actorHandleId;

  // Id for createActor a target actor
  public UniqueId createActorId;

  // The task's resource demands.
  public Map<String, Double> resources;

  public UniqueId cursorId;

  public TaskSpec() {}

  public TaskSpec(UniqueId driverId, UniqueId taskId, UniqueId parentTaskId, int parentCounter,
      UniqueId actorId, int actorCounter, UniqueId functionId, FunctionArg[] args,
      UniqueId[] returnIds, UniqueId actorHandleId, UniqueId createActorId,
      Map<String, Double> resources, UniqueId cursorId) {
    this.driverId = driverId;
    this.taskId = taskId;
    this.parentTaskId = parentTaskId;
    this.parentCounter = parentCounter;
    this.actorId = actorId;
    this.actorCounter = actorCounter;
    this.functionId = functionId;
    this.args = args;
    this.returnIds = returnIds;
    this.actorHandleId = actorHandleId;
    this.createActorId = createActorId;
    this.resources = resources;
    this.cursorId = cursorId;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("\ttaskId: ").append(taskId).append("\n");
    builder.append("\tdriverId: ").append(driverId).append("\n");
    builder.append("\tparentCounter: ").append(parentCounter).append("\n");
    builder.append("\tactorId: ").append(actorId).append("\n");
    builder.append("\tactorCounter: ").append(actorCounter).append("\n");
    builder.append("\tfunctionId: ").append(functionId).append("\n");
    builder.append("\treturnIds: ").append(Arrays.toString(returnIds)).append("\n");
    builder.append("\tactorHandleId: ").append(actorHandleId).append("\n");
    builder.append("\tcreateActorId: ").append(createActorId).append("\n");
    builder.append("\tresources: ")
        .append(ResourceUtil.getResourcesFromatStringFromMap(resources)).append("\n");
    builder.append("\tcursorId: ").append(cursorId).append("\n");
    builder.append("\targs:\n");
    for (FunctionArg arg : args) {
      builder.append("\t\t");
      arg.toString(builder);
      builder.append("\n");
    }
    return builder.toString();
  }

}
