package org.ray.runtime;

import org.ray.api.exception.RayException;
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.RayFunction;
import org.ray.runtime.task.ArgumentsBuilder;
import org.ray.runtime.task.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The worker, which pulls tasks from {@link org.ray.runtime.raylet.RayletClient} and executes them
 * continuously.
 */
public class Worker {

  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  private final AbstractRayRuntime runtime;

  public Worker(AbstractRayRuntime runtime) {
    this.runtime = runtime;
  }

  public void loop() {
    while (true) {
      LOGGER.info("Fetching new task in thread {}.", Thread.currentThread().getName());
      TaskSpec task = runtime.getRayletClient().getTask();
      execute(task);
    }
  }

  /**
   * Execute a task.
   */
  public void execute(TaskSpec spec) {
    LOGGER.info("Executing task {}", spec.taskId);
    LOGGER.debug("Executing task {}", spec);
    UniqueId returnId = spec.returnIds[0];
    ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Get method
      RayFunction rayFunction = runtime.getFunctionManager()
          .getFunction(spec.driverId, spec.functionDescriptor);
      // Set context
      runtime.getWorkerContext().setCurrentTask(spec, rayFunction.classLoader);
      Thread.currentThread().setContextClassLoader(rayFunction.classLoader);
      // Get local actor object and arguments.
      Object actor = spec.isActorTask() ? runtime.localActors.get(spec.actorId) : null;
      Object[] args = ArgumentsBuilder.unwrap(spec, rayFunction.classLoader);
      // Execute the task.
      Object result;
      if (!rayFunction.isConstructor()) {
        result = rayFunction.getMethod().invoke(actor, args);
      } else {
        result = rayFunction.getConstructor().newInstance(args);
      }
      // Set result
      if (!spec.isActorCreationTask()) {
        runtime.put(returnId, result);
      } else {
        runtime.localActors.put(returnId, result);
      }
      LOGGER.info("Finished executing task {}", spec.taskId);
    } catch (Exception e) {
      LOGGER.error("Error executing task " + spec, e);
      runtime.put(returnId, new RayException("Error executing task " + spec, e));
    } finally {
      runtime.getWorkerContext().setCurrentTask(null, null);
      Thread.currentThread().setContextClassLoader(oldLoader);
    }
  }
}
