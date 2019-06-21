package org.ray.runtime;

import org.ray.api.id.UniqueId;
import org.ray.runtime.task.TaskInfo;
import org.ray.runtime.util.RayObjectValueConverter;

public class WorkerContext {
  private ClassLoader currentClassLoader;

  private RayObjectValueConverter rayObjectValueConverter = new RayObjectValueConverter(null);

  private TaskInfo currentTask;

  private UniqueId currentActorId;

  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  public void setCurrentClassLoader(ClassLoader currentClassLoader) {
    if (this.currentClassLoader != currentClassLoader) {
      this.currentClassLoader = currentClassLoader;
      rayObjectValueConverter = new RayObjectValueConverter(currentClassLoader);
    }
  }

  public RayObjectValueConverter getRayObjectValueConverter() {
    return rayObjectValueConverter;
  }

  public TaskInfo getCurrentTask() {
    return currentTask;
  }

  public void setCurrentTask(TaskInfo currentTask) {
    this.currentTask = currentTask;
  }

  public UniqueId getCurrentActorId() {
    return currentActorId;
  }

  public void setCurrentActorId(UniqueId currentActorId) {
    this.currentActorId = currentActorId;
  }
}
