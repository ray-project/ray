package org.ray.runtime.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.util.ResourceUtil;

/**
 * Represents necessary information of a task for scheduling and executing.
 */
public class TaskSpec {

  // ID of the driver that created this task.
  public final UniqueId driverId;

  // Task ID of the task.
  public final UniqueId taskId;

  // Task ID of the parent task.
  public final UniqueId parentTaskId;

  // A count of the number of tasks submitted by the parent task before this one.
  public final int parentCounter;

  // Id for createActor a target actor
  public final UniqueId actorCreationId;

  public final int maxActorReconstructions;

  // Actor ID of the task. This is the actor that this task is executed on
  // or NIL_ACTOR_ID if the task is just a normal task.
  public final UniqueId actorId;

  // ID per actor client for session consistency
  public final UniqueId actorHandleId;

  // Number of tasks that have been submitted to this actor so far.
  public final int actorCounter;

  // Task arguments.
  public final FunctionArg[] args;

  // return ids
  public final UniqueId[] returnIds;

  // The task's resource demands.
  public final Map<String, Double> resources;

  // Function descriptor is a list of strings that can uniquely identify a function.
  // It will be sent to worker and used to load the target callable function.
  public final FunctionDescriptor functionDescriptor;

  private List<UniqueId> executionDependencies;

  public boolean isActorTask() {
    return !actorId.isNil();
  }

  public boolean isActorCreationTask() {
    return !actorCreationId.isNil();
  }

  public TaskSpec(UniqueId driverId, UniqueId taskId, UniqueId parentTaskId, int parentCounter,
      UniqueId actorCreationId, int maxActorReconstructions, UniqueId actorId,
      UniqueId actorHandleId, int actorCounter, FunctionArg[] args, UniqueId[] returnIds,
      Map<String, Double> resources, FunctionDescriptor functionDescriptor) {
    this.driverId = driverId;
    this.taskId = taskId;
    this.parentTaskId = parentTaskId;
    this.parentCounter = parentCounter;
    this.actorCreationId = actorCreationId;
    this.maxActorReconstructions = maxActorReconstructions;
    this.actorId = actorId;
    this.actorHandleId = actorHandleId;
    this.actorCounter = actorCounter;
    this.args = args;
    this.returnIds = returnIds;
    this.resources = resources;
    this.functionDescriptor = functionDescriptor;
    this.executionDependencies = new ArrayList<>();
  }

  public List<UniqueId> getExecutionDependencies() {
    return executionDependencies;
  }

  @Override
  public String toString() {
    return "TaskSpec{" +
        "driverId=" + driverId +
        ", taskId=" + taskId +
        ", parentTaskId=" + parentTaskId +
        ", parentCounter=" + parentCounter +
        ", actorCreationId=" + actorCreationId +
        ", actorId=" + actorId +
        ", actorHandleId=" + actorHandleId +
        ", actorCounter=" + actorCounter +
        ", args=" + Arrays.toString(args) +
        ", returnIds=" + Arrays.toString(returnIds) +
        ", resources=" + ResourceUtil.getResourcesStringFromMap(resources) +
        ", functionDescriptor=" + functionDescriptor +
        '}';
  }
}
