package org.ray.runtime.task;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.functionmanager.PyFunctionDescriptor;

/**
 * Represents necessary information of a task for scheduling and executing.
 */
public class TaskSpec {

  // ID of the job that created this task.
  public final JobId jobId;

  // Task ID of the task.
  public final TaskId taskId;

  // Task ID of the parent task.
  public final TaskId parentTaskId;

  // A count of the number of tasks submitted by the parent task before this one.
  public final int parentCounter;

  // Id for createActor a target actor
  public final ActorId actorCreationId;

  public final int maxActorReconstructions;

  // Actor ID of the task. This is the actor that this task is executed on
  // or NIL_ACTOR_ID if the task is just a normal task.
  public final ActorId actorId;

  // ID per actor client for session consistency
  public final UniqueId actorHandleId;

  // Number of tasks that have been submitted to this actor so far.
  public final int actorCounter;

  // Object id returned by the previous task submitted to the same actor.
  public final ObjectId previousActorTaskDummyObjectId;

  // Task arguments.
  public final UniqueId[] newActorHandles;

  // Task arguments.
  public final FunctionArg[] args;

  // number of return objects.
  public final int numReturns;

  // Return ids.
  public final ObjectId[] returnIds;

  // The task's resource demands.
  public final Map<String, Double> resources;

  // Language of this task.
  public final TaskLanguage language;

  public final List<String> dynamicWorkerOptions;

  // Descriptor of the remote function.
  // Note, if task language is Java, the type is JavaFunctionDescriptor. If the task language
  // is Python, the type is PyFunctionDescriptor.
  private final FunctionDescriptor functionDescriptor;

  public boolean isActorTask() {
    return !actorId.isNil();
  }

  public boolean isActorCreationTask() {
    return !actorCreationId.isNil();
  }

  public TaskSpec(
      JobId jobId,
      TaskId taskId,
      TaskId parentTaskId,
      int parentCounter,
      ActorId actorCreationId,
      int maxActorReconstructions,
      ActorId actorId,
      UniqueId actorHandleId,
      int actorCounter,
      ObjectId previousActorTaskDummyObjectId,
      UniqueId[] newActorHandles,
      FunctionArg[] args,
      int numReturns,
      Map<String, Double> resources,
      TaskLanguage language,
      FunctionDescriptor functionDescriptor,
      List<String> dynamicWorkerOptions) {
    this.jobId = jobId;
    this.taskId = taskId;
    this.parentTaskId = parentTaskId;
    this.parentCounter = parentCounter;
    this.actorCreationId = actorCreationId;
    this.maxActorReconstructions = maxActorReconstructions;
    this.actorId = actorId;
    this.actorHandleId = actorHandleId;
    this.actorCounter = actorCounter;
    this.previousActorTaskDummyObjectId = previousActorTaskDummyObjectId;
    this.newActorHandles = newActorHandles;
    this.args = args;
    this.numReturns = numReturns;
    this.dynamicWorkerOptions = dynamicWorkerOptions;

    returnIds = new ObjectId[numReturns];
    for (int i = 0; i < numReturns; ++i) {
      returnIds[i] = ObjectId.forReturn(taskId, i + 1);
    }
    this.resources = resources;
    this.language = language;
    if (language == TaskLanguage.JAVA) {
      Preconditions.checkArgument(functionDescriptor instanceof JavaFunctionDescriptor,
          "Expect JavaFunctionDescriptor type, but got {}.", functionDescriptor.getClass());
    } else if (language == TaskLanguage.PYTHON) {
      Preconditions.checkArgument(functionDescriptor instanceof PyFunctionDescriptor,
          "Expect PyFunctionDescriptor type, but got {}.", functionDescriptor.getClass());
    } else {
      Preconditions.checkArgument(false, "Unknown task language: {}.", language);
    }
    this.functionDescriptor = functionDescriptor;
  }

  public JavaFunctionDescriptor getJavaFunctionDescriptor() {
    Preconditions.checkState(language == TaskLanguage.JAVA);
    return (JavaFunctionDescriptor) functionDescriptor;
  }

  public PyFunctionDescriptor getPyFunctionDescriptor() {
    Preconditions.checkState(language == TaskLanguage.PYTHON);
    return (PyFunctionDescriptor) functionDescriptor;
  }

  @Override
  public String toString() {
    return "TaskSpec{" +
        "jobId=" + jobId +
        ", taskId=" + taskId +
        ", parentTaskId=" + parentTaskId +
        ", parentCounter=" + parentCounter +
        ", actorCreationId=" + actorCreationId +
        ", maxActorReconstructions=" + maxActorReconstructions +
        ", actorId=" + actorId +
        ", actorHandleId=" + actorHandleId +
        ", actorCounter=" + actorCounter +
        ", previousActorTaskDummyObjectId=" + previousActorTaskDummyObjectId +
        ", newActorHandles=" + Arrays.toString(newActorHandles) +
        ", args=" + Arrays.toString(args) +
        ", numReturns=" + numReturns +
        ", resources=" + resources +
        ", language=" + language +
        ", functionDescriptor=" + functionDescriptor +
	", dynamicWorkerOptions=" + dynamicWorkerOptions +
        '}';
  }
}
