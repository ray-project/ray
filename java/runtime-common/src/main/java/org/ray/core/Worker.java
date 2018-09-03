package org.ray.core;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.Ray;
import org.ray.api.exception.RayException;
import org.ray.api.id.UniqueId;
import org.ray.api.runtime.RayRuntime;
import org.ray.spi.LocalSchedulerLink;
import org.ray.spi.model.RayMethod;
import org.ray.spi.model.TaskSpec;
import org.ray.util.logger.RayLog;

/**
 * The worker, which pulls tasks from {@code org.ray.spi.LocalSchedulerProxy} and executes them
 * continuously.
 */
public class Worker {

  private final AbstractRayRuntime runtime;

  public Worker(AbstractRayRuntime runtime) {
    this.runtime = runtime;
  }

  public void loop() {
    while (true) {
      RayLog.core.info(Thread.currentThread().getName() + ":fetching new task...");
      TaskSpec task = runtime.getLocalSchedulerClient().getTask();
      execute(task);
    }
  }

  /**
   * Execute a task.
   */
  public void execute(TaskSpec task) {
    RayLog.core.info("Executing task {}", task.taskId);
    Pair<ClassLoader, RayMethod> pair = runtime.getLocalFunctionManager().getMethod(
        task.driverId, task.actorId, task.functionId, task.args);
    doExecute(task, pair.getRight(), pair.getLeft());
  }

  private void doExecute(TaskSpec spec, RayMethod method, ClassLoader classLoader) {
    UniqueId returnId = spec.returnIds[0];
    WorkerContext.prepare(spec, classLoader);
    ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(classLoader);
      // Get local actor object and arguments.
      Object actor = spec.isActorTask() ? runtime.localActors.get(spec.actorId) : null;
      Object[] args = ArgumentsBuilder.unwrap(spec, classLoader);
      // Execute the task.
      Object result;
      if (method.invokable instanceof Method) {
        result = ((Method) method.invokable).invoke(actor, args);
      }
      else {
        result = ((Constructor) method.invokable).newInstance(args);
      }
      // Set result
      if (!spec.isActorCreationTask()) {
        runtime.put(returnId, result);
      }
      else {
        runtime.localActors.put(returnId, result);
      }
      RayLog.core.info("Finished executing task {}", spec.taskId);
    } catch (Exception e) {
      RayLog.core.error("Error executing task " + spec, e);
      runtime.put(returnId, new RayException("Error executing task " + spec, e));
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
    }
  }
}
