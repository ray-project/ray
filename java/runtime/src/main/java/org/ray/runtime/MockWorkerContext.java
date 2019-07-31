package org.ray.runtime;

import org.ray.api.id.JobId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.TaskSpec;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MockWorkerContext implements WorkerContext {

  private final JobId jobId;
  private ThreadLocal<TaskSpec> currentTask = new ThreadLocal<>();
  private ClassLoader currentClassLoader;

  public MockWorkerContext(JobId jobId) {
    this.jobId = jobId;
  }

  @Override
  public UniqueId getCurrentWorkerId() {
    throw new NotImplementedException();
  }

  @Override
  public JobId getCurrentJobId() {
    return jobId;
  }

  @Override
  public UniqueId getCurrentActorId() {
    TaskSpec taskSpec = currentTask.get();
    if (taskSpec == null) {
      return UniqueId.NIL;
    }
    return MockTaskInterface.getActorId(taskSpec);
  }

  @Override
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  @Override
  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    this.currentClassLoader = currentClassLoader;
  }

  @Override
  public TaskSpec getCurrentTask() {
    return currentTask.get();
  }

  public void setCurrentTask(TaskSpec taskSpec) {
    currentTask.set(taskSpec);
  }
}
