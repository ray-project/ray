package org.ray.runtime;

import com.google.common.base.Preconditions;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.WorkerMode;
import org.ray.runtime.task.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerContext.class);

  private UniqueId workerId;

  private ThreadLocal<UniqueId> currentTaskId;

  /**
   * Number of objects that have been put from current task.
   */
  private ThreadLocal<Integer> putIndex;

  /**
   * Number of tasks that have been submitted from current task.
   */
  private ThreadLocal<Integer> taskIndex;

  private UniqueId currentDriverId;

  private ClassLoader currentClassLoader;

  /**
   * The ID of main thread which created the worker context.
   */
  private long mainThreadId;


  public WorkerContext(WorkerMode workerMode, UniqueId driverId) {
    mainThreadId = Thread.currentThread().getId();
    taskIndex = ThreadLocal.withInitial(() -> 0);
    putIndex = ThreadLocal.withInitial(() -> 0);
    currentTaskId = ThreadLocal.withInitial(UniqueId::randomId);
    if (workerMode == WorkerMode.DRIVER) {
      workerId = driverId;
      currentTaskId.set(UniqueId.randomId());
      currentDriverId = driverId;
      currentClassLoader = null;
    } else {
      workerId = UniqueId.randomId();
      setCurrentTask(null, null);
    }
  }

  /**
   * @return For the main thread, this method returns the ID of this worker's current running task;
   *     for other threads, this method returns a random ID.
   */
  public UniqueId getCurrentTaskId() {
    return currentTaskId.get();
  }

  /**
   * Set the current task which is being executed by the current worker. Note, this method can only
   * be called from the main thread.
   */
  public void setCurrentTask(TaskSpec task, ClassLoader classLoader) {
    Preconditions.checkState(
        Thread.currentThread().getId() == mainThreadId,
        "This method should only be called from the main thread."
    );
    if (task != null) {
      currentTaskId.set(task.taskId);
      currentDriverId = task.driverId;
    } else {
      currentTaskId.set(UniqueId.NIL);
      currentDriverId = UniqueId.NIL;
    }
    taskIndex.set(0);
    putIndex.set(0);
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
   * @return If this worker is a driver, this method returns the driver ID; Otherwise, it returns
   *     the driver ID of the current running task.
   */
  public UniqueId getCurrentDriverId() {
    return currentDriverId;
  }

  /**
   * @return The class loader which is associated with the current driver.
   */
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

}
