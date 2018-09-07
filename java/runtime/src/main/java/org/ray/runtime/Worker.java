package org.ray.runtime;

import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.exception.RayException;
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.RayMethod;
import org.ray.runtime.task.TaskSpec;
import org.ray.runtime.util.ArgumentsBuilder;
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
  public void execute(TaskSpec spec) {
    RayLog.core.info("Executing task {}", spec.taskId);
    UniqueId returnId = spec.returnIds[0];
    ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Get method
      Pair<ClassLoader, RayMethod> pair = runtime.getLocalFunctionManager().getMethod(
          spec.driverId, spec.actorId, spec.functionId, spec.args);
      ClassLoader classLoader = pair.getLeft();
      RayMethod method = pair.getRight();
      // Set context
      WorkerContext.prepare(spec, classLoader);
      Thread.currentThread().setContextClassLoader(classLoader);
      // Get local actor object and arguments.
      Object actor = spec.isActorTask() ? runtime.localActors.get(spec.actorId) : null;
      Object[] args = ArgumentsBuilder.unwrap(spec, classLoader);
      // Execute the task.
      Object result;
      if (!method.isConstructor()) {
        result = method.getMethod().invoke(actor, args);
      } else {
        result = method.getConstructor().newInstance(args);
      }
      // Set result
      if (!spec.isActorCreationTask()) {
        runtime.put(returnId, result);
      } else {
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
