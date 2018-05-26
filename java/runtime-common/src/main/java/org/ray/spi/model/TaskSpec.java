package org.ray.spi.model;

import java.util.Arrays;
import org.ray.api.UniqueID;

/**
 * Represents necessary information of a task for scheduling and executing.
 */
public class TaskSpec {

  // ID of the driver that created this task.
  public UniqueID driverId;

  // Task ID of the task.
  public UniqueID taskId;

  // Task ID of the parent task.
  public UniqueID parentTaskId;

  // A count of the number of tasks submitted by the parent task before this one.
  public int parentCounter;

  // Actor ID of the task. This is the actor that this task is executed on
  // or NIL_ACTOR_ID if the task is just a normal task.
  public UniqueID actorId;

  // Number of tasks that have been submitted to this actor so far.
  public int actorCounter;

  // Function ID of the task.
  public UniqueID functionId;

  // Task arguments.
  public FunctionArg[] args;

  // return ids
  public UniqueID[] returnIds;

  // ID per actor client for session consistency
  public UniqueID actorHandleId;

  // Id for create a target actor
  public UniqueID createActorId;

  public UniqueID cursorId;

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