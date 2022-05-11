package io.ray.runtime.context;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.generated.Common.Address;
import io.ray.runtime.generated.Common.TaskSpec;
import io.ray.runtime.generated.Common.TaskType;
import io.ray.runtime.task.LocalModeTaskSubmitter;
import java.util.Random;

/** Worker context for local mode. */
public class LocalModeWorkerContext implements WorkerContext {

  private final JobId jobId;
  private ThreadLocal<TaskSpec> currentTask = new ThreadLocal<>();
  private final ThreadLocal<UniqueId> currentWorkerId = new ThreadLocal<>();

  public LocalModeWorkerContext(JobId jobId) {
    this.jobId = jobId;

    // Create a dummy driver task with a random task id, so that we can call
    // `getCurrentTaskId` from a driver.
    byte[] driverTaskId = new byte[TaskId.LENGTH];
    new Random().nextBytes(driverTaskId);
    TaskSpec dummyDriverTask =
        TaskSpec.newBuilder().setTaskId(ByteString.copyFrom(driverTaskId)).build();
    currentTask.set(dummyDriverTask);
  }

  @Override
  public UniqueId getCurrentWorkerId() {
    return currentWorkerId.get();
  }

  public void setCurrentWorkerId(UniqueId workerId) {
    currentWorkerId.set(workerId);
  }

  @Override
  public JobId getCurrentJobId() {
    return jobId;
  }

  @Override
  public ActorId getCurrentActorId() {
    TaskSpec taskSpec = currentTask.get();
    checkTaskSpecNotNull(taskSpec);
    return LocalModeTaskSubmitter.getActorId(taskSpec);
  }

  @Override
  public TaskType getCurrentTaskType() {
    TaskSpec taskSpec = currentTask.get();
    checkTaskSpecNotNull(taskSpec);
    return taskSpec.getType();
  }

  @Override
  public TaskId getCurrentTaskId() {
    TaskSpec taskSpec = currentTask.get();
    checkTaskSpecNotNull(taskSpec);
    return TaskId.fromBytes(taskSpec.getTaskId().toByteArray());
  }

  @Override
  public Address getRpcAddress() {
    return Address.getDefaultInstance();
  }

  public void setCurrentTask(TaskSpec taskSpec) {
    currentTask.set(taskSpec);
  }

  private static void checkTaskSpecNotNull(TaskSpec taskSpec) {
    Preconditions.checkNotNull(
        taskSpec,
        "Current task is not set. Maybe you invoked this API in a user-created thread not managed by Ray. Invoking this API in a user-created thread is not supported yet in local mode. You can switch to cluster mode.");
  }
}
