package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.WorkerMode;
import org.ray.runtime.task.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerContext.class);

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

  /**
   * The ID of main thread which created the worker context.
   */
  private long mainThreadId;
  /**
   * If the multi-threading warning message has been logged.
   */
  private AtomicBoolean multiThreadingWarned;

  public WorkerContext(WorkerMode workerMode, UniqueId driverId) {
    workerId = workerMode == WorkerMode.DRIVER ? driverId : UniqueId.randomId();
    currentTaskPutCount = new AtomicInteger(0);
    currentTaskCallCount = new AtomicInteger(0);
    currentClassLoader = null;
    currentTask = createDummyTask(workerMode, driverId);
    mainThreadId = Thread.currentThread().getId();
    multiThreadingWarned = new AtomicBoolean();
  }

  /**
   * Get the current thread's task ID.
   * This returns the assigned task ID if called on the main thread, else a
   * random task ID.
   */
  public UniqueId getCurrentThreadTaskId() {
    UniqueId taskId;
    if (Thread.currentThread().getId() == mainThreadId) {
      taskId = currentTask.taskId;
    } else {
      taskId = UniqueId.randomId();
      if (multiThreadingWarned.compareAndSet(false, true)) {
        LOGGER.warn("Calling Ray.get or Ray.wait in a separate thread " +
            "may lead to deadlock if the main thread blocks on this " +
            "thread and there are not enough resources to execute " +
            "more tasks");
      }
    }

    Preconditions.checkState(!taskId.isNil());
    return taskId;
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
