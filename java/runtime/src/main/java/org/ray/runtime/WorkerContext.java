package org.ray.runtime;

import com.google.common.base.Preconditions;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.config.WorkerMode;
import org.ray.runtime.task.TaskSpec;
import org.ray.runtime.util.IdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerContext.class);

  private UniqueId workerId;

  private ThreadLocal<TaskId> currentTaskId;

  /**
   * Number of objects that have been put from current task.
   */
  private ThreadLocal<Integer> putIndex;

  /**
   * Number of tasks that have been submitted from current task.
   */
  private ThreadLocal<Integer> taskIndex;

  private ThreadLocal<TaskSpec> currentTask;

  private JobId currentJobId;

  private ClassLoader currentClassLoader;

  /**
   * The ID of main thread which created the worker context.
   */
  private long mainThreadId;

  /**
   * The run-mode of this worker.
   */
  private RunMode runMode;

  public WorkerContext(WorkerMode workerMode, JobId jobId, RunMode runMode) {
    mainThreadId = Thread.currentThread().getId();
    taskIndex = ThreadLocal.withInitial(() -> 0);
    putIndex = ThreadLocal.withInitial(() -> 0);
    currentTaskId = ThreadLocal.withInitial(TaskId::randomId);
    this.runMode = runMode;
    currentTask = ThreadLocal.withInitial(() -> null);
    currentClassLoader = null;
    if (workerMode == WorkerMode.DRIVER) {
      workerId = IdUtil.computeDriverId(jobId);
      currentTaskId.set(TaskId.randomId());
      currentJobId = jobId;
    } else {
      workerId = UniqueId.randomId();
      this.currentTaskId.set(TaskId.NIL);
      this.currentJobId = JobId.NIL;
    }
  }

  /**
   * @return For the main thread, this method returns the ID of this worker's current running task;
   *     for other threads, this method returns a random ID.
   */
  public TaskId getCurrentTaskId() {
    return currentTaskId.get();
  }

  /**
   * Set the current task which is being executed by the current worker. Note, this method can only
   * be called from the main thread.
   */
  public void setCurrentTask(TaskSpec task, ClassLoader classLoader) {
    if (runMode == RunMode.CLUSTER) {
      Preconditions.checkState(
              Thread.currentThread().getId() == mainThreadId,
              "This method should only be called from the main thread."
      );
    }

    Preconditions.checkNotNull(task);
    this.currentTaskId.set(task.taskId);
    this.currentJobId = task.jobId;
    taskIndex.set(0);
    putIndex.set(0);
    this.currentTask.set(task);
    currentClassLoader = classLoader;
  }

  /**
   * Increment the put index and return the new value.
   */
  public int nextPutIndex() {
    putIndex.set(putIndex.get() + 1);
    return putIndex.get();
  }

  /**
   * Increment the task index and return the new value.
   */
  public int nextTaskIndex() {
    taskIndex.set(taskIndex.get() + 1);
    return taskIndex.get();
  }

  /**
   * @return The ID of the current worker.
   */
  public UniqueId getCurrentWorkerId() {
    return workerId;
  }

  /**
   * The ID of the current job.
   */
  public JobId getCurrentJobId() {
    return currentJobId;
  }

  /**
   * @return The class loader which is associated with the current job.
   */
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  /**
   * Get the current task.
   */
  public TaskSpec getCurrentTask() {
    return this.currentTask.get();
  }
}
