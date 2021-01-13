package io.ray.runtime.context;

import com.google.common.base.Preconditions;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.generated.Common.Address;
import io.ray.runtime.generated.Common.TaskSpec;
import io.ray.runtime.generated.Common.TaskType;
import io.ray.runtime.task.LocalModeTaskSubmitter;

/** Worker context for local mode. */
public class LocalModeWorkerContext implements WorkerContext {

  private final JobId jobId;
  private ThreadLocal<TaskSpec> currentTask = new ThreadLocal<>();
  private final ThreadLocal<UniqueId> currentWorkerId = new ThreadLocal<>();

  public LocalModeWorkerContext(JobId jobId) {
    this.jobId = jobId;
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
    if (taskSpec == null) {
      return ActorId.NIL;
    }
    return LocalModeTaskSubmitter.getActorId(taskSpec);
  }

  @Override
  public ClassLoader getCurrentClassLoader() {
    return null;
  }

  @Override
  public void setCurrentClassLoader(ClassLoader currentClassLoader) {}

  @Override
  public TaskType getCurrentTaskType() {
    TaskSpec taskSpec = currentTask.get();
    Preconditions.checkNotNull(taskSpec, "Current task is not set.");
    return taskSpec.getType();
  }

  @Override
  public TaskId getCurrentTaskId() {
    TaskSpec taskSpec = currentTask.get();
    Preconditions.checkState(taskSpec != null);
    return TaskId.fromBytes(taskSpec.getTaskId().toByteArray());
  }

  @Override
  public Address getRpcAddress() {
    return Address.getDefaultInstance();
  }

  public void setCurrentTask(TaskSpec taskSpec) {
    currentTask.set(taskSpec);
  }
}
