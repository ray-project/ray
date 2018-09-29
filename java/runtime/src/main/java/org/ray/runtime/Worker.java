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

  private final Logger logger = LoggerFactory.getLogger(Worker.class);

  private final AbstractRayRuntime runtime;

  public Worker(AbstractRayRuntime runtime) {
    this.runtime = runtime;
  }

  public void loop() {
    while (true) {
      logger.info("Fetching new task in thread {}.", Thread.currentThread().getName());
      TaskSpec task = runtime.getRayletClient().getTask();
      execute(task);
    }
  }

  /**
   * Execute a task.
   */
  public void execute(TaskSpec spec) {
    logger.info("Executing task {}", spec.taskId);
    logger.debug("Executing task {}", spec);
    UniqueId returnId = spec.returnIds[0];
    ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Get method
      RayFunction rayFunction = runtime.getFunctionManager()
          .getFunction(spec.driverId, spec.functionDescriptor);
      // Set context
      runtime.getWorkerContext().setCurrentTask(spec);
      runtime.getWorkerContext().setCurrentClassLoader(rayFunction.classLoader);
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
      logger.info("Finished executing task {}", spec.taskId);
    } catch (Exception e) {
      logger.error("Error executing task " + spec, e);
      runtime.put(returnId, new RayException("Error executing task " + spec, e));
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
    }
  }
}
