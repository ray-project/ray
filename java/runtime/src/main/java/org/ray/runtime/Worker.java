package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
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

  /**
   * The current actor object, if this worker is an actor, otherwise null.
   */
  private Object currentActor = null;

  /**
   * Id of the current actor object, if the worker is an actor, otherwise NIL.
   */
  private UniqueId currentActorId = UniqueId.NIL;

  /**
   * The exception that failed the actor creation task, if any.
   */
  private Exception actorCreationException = null;

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
      Object actor = null;
      if (spec.isActorTask()) {
        Preconditions.checkState(spec.actorId.equals(currentActorId));
        if (actorCreationException != null) {
          throw actorCreationException;
        }
        actor = currentActor;
      }
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
        currentActor = result;
        currentActorId = returnId;
      }
      LOGGER.info("Finished executing task {}", spec.taskId);
    } catch (Exception e) {
      LOGGER.error("Error executing task " + spec, e);
      if (!spec.isActorCreationTask()) {
        runtime.put(returnId, new RayException("Error executing task " + spec, e));
      } else {
        actorCreationException = e;
        currentActorId = returnId;
      }
    } finally {
      runtime.getWorkerContext().setCurrentTask(null, null);
      Thread.currentThread().setContextClassLoader(oldLoader);
    }
  }
}
