package org.ray.runtime;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.ray.api.id.UniqueId;
import org.ray.runtime.config.WorkerMode;
import org.ray.runtime.task.TaskSpec;

public class WorkerContext {

  /**
   * Worker id.
   */
  private UniqueId workerId;

  /**
   * Current task.
   */
  private TaskSpec currentTask;

  /**
   * Current class loader.
   */
  private ClassLoader currentClassLoader;

  /**
   * How many puts have been done by current task.
   */
  private AtomicInteger currentTaskPutCount;

  /**
   * How many calls have been done by current task.
   */
  private AtomicInteger currentTaskCallCount;

  public WorkerContext(WorkerMode workerMode, UniqueId driverId) {
    workerId = workerMode == WorkerMode.DRIVER ? driverId : UniqueId.randomId();
    currentTaskPutCount = new AtomicInteger(0);
    currentTaskCallCount = new AtomicInteger(0);
    currentClassLoader = null;
    currentTask = createDummyTask(workerMode, driverId);
  }

  public void setWorkerId(UniqueId workerId) {
    this.workerId = workerId;
  }

  public TaskSpec getCurrentTask() {
    return currentTask;
  }

  public int nextPutIndex() {
    return currentTaskPutCount.incrementAndGet();
  }

  public int nextCallIndex() {
    return currentTaskCallCount.incrementAndGet();
  }

  public UniqueId getCurrentWorkerId() {
    return workerId;
  }

  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  public void setCurrentTask(TaskSpec currentTask) {
    this.currentTask = currentTask;
  }

  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    this.currentClassLoader = currentClassLoader;
  }

  private TaskSpec createDummyTask(WorkerMode workerMode, UniqueId driverId) {
    return new TaskSpec(
        driverId,
        workerMode == WorkerMode.DRIVER ? UniqueId.randomId() : UniqueId.NIL,
        UniqueId.NIL,
        0,
        UniqueId.NIL,
        0,
        UniqueId.NIL,
        UniqueId.NIL,
        0,
        null,
        null,
        new HashMap<>(),
        null);
  }
}
