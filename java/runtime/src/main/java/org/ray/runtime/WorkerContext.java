package org.ray.runtime;

import org.ray.api.id.UniqueId;
import org.ray.runtime.task.TaskInfo;
import org.ray.runtime.util.RayObjectConverter;

public class WorkerContext {
  private final long nativeCoreWorker;

  private ClassLoader currentClassLoader;

  private RayObjectConverter rayObjectConverter = new RayObjectConverter(null);

  private TaskInfo currentTask;

  public WorkerContext(long nativeCoreWorker) {
    this.nativeCoreWorker = nativeCoreWorker;
  }

  public RayObjectConverter getRayObjectConverter() {
    return rayObjectConverter;
  }

  /**
   * The ID of the current job.
   */
  public UniqueId getCurrentJobId() {
    return new UniqueId(nativeGetCurrentJobId(nativeCoreWorker));
  }

  /**
   * @return The ID of the current worker.
   */
  public UniqueId getCurrentActorId() {
    return new UniqueId(nativeGetCurrentActorId(nativeCoreWorker));
  }

  public UniqueId getCurrentWorkerId() {
    return new UniqueId(nativeGetCurrentWorkerId(nativeCoreWorker));
  }

  /**
   * @return The class loader which is associated with the current job.
   */
  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    if (this.currentClassLoader != currentClassLoader) {
      this.currentClassLoader = currentClassLoader;
      rayObjectConverter = new RayObjectConverter(currentClassLoader);
    }
  }

  /**
   * Get the current task.
   */
  public TaskInfo getCurrentTask() {
    return currentTask;
  }

  public void setCurrentTask(TaskInfo currentTask) {
    this.currentTask = currentTask;
  }

  private static native byte[] nativeGetCurrentJobId(long nativeCoreWorker);

  private static native byte[] nativeGetCurrentWorkerId(long nativeCoreWorker);

  private static native byte[] nativeGetCurrentActorId(long nativeCoreWorker);
}
