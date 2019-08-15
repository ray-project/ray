package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.Checkpointable;
import org.ray.api.Checkpointable.Checkpoint;
import org.ray.api.Checkpointable.CheckpointContext;
import org.ray.api.exception.RayTaskException;
import org.ray.api.id.ActorId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.functionmanager.RayFunction;
import org.ray.runtime.task.ArgumentsBuilder;
import org.ray.runtime.task.TaskSpec;
import org.ray.runtime.util.IdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The worker, which pulls tasks from {@link org.ray.runtime.raylet.RayletClient} and executes them
 * continuously.
 */
public class Worker {

  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  // TODO(hchen): Use the C++ config.
  private static final int NUM_ACTOR_CHECKPOINTS_TO_KEEP = 20;

  private final AbstractRayRuntime runtime;

  /**
   * The current actor object, if this worker is an actor, otherwise null.
   */
  private Object currentActor = null;

  /**
   * Id of the current actor object, if the worker is an actor, otherwise NIL.
   */
  private ActorId currentActorId = ActorId.NIL;

  /**
   * The exception that failed the actor creation task, if any.
   */
  private Exception actorCreationException = null;

  /**
   * Number of tasks executed since last actor checkpoint.
   */
  private int numTasksSinceLastCheckpoint = 0;

  /**
   * IDs of this actor's previous checkpoints.
   */
  private List<UniqueId> checkpointIds;

  /**
   * Timestamp of the last actor checkpoint.
   */
  private long lastCheckpointTimestamp = 0;


  public Worker(AbstractRayRuntime runtime) {
    this.runtime = runtime;
  }

  public ActorId getCurrentActorId() {
    return currentActorId;
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
    LOGGER.debug("Executing task {}", spec);
    ObjectId returnId = spec.returnIds[0];
    ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Get method
      RayFunction rayFunction = runtime.getFunctionManager()
          .getFunction(spec.jobId, spec.getJavaFunctionDescriptor());
      // Set context
      runtime.getWorkerContext().setCurrentTask(spec, rayFunction.classLoader);
      Thread.currentThread().setContextClassLoader(rayFunction.classLoader);

      if (spec.isActorCreationTask()) {
        currentActorId = spec.taskId.getActorId();
      }

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
        if (spec.isActorTask()) {
          maybeSaveCheckpoint(actor, spec.actorId);
        }

        runtime.put(returnId, result);
      } else {
        maybeLoadCheckpoint(result, spec.taskId.getActorId());
        currentActor = result;
      }
      LOGGER.debug("Finished executing task {}", spec.taskId);
    } catch (Exception e) {
      LOGGER.error("Error executing task " + spec, e);
      if (!spec.isActorCreationTask()) {
        runtime.put(returnId, new RayTaskException("Error executing task " + spec, e));
      } else {
        actorCreationException = e;
      }
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
    }
  }

  private void maybeSaveCheckpoint(Object actor, ActorId actorId) {
    if (!(actor instanceof Checkpointable)) {
      return;
    }
    if (runtime.getRayConfig().runMode == RunMode.SINGLE_PROCESS) {
      // Actor checkpointing isn't implemented for SINGLE_PROCESS mode yet.
      return;
    }
    CheckpointContext checkpointContext = new CheckpointContext(actorId,
        ++numTasksSinceLastCheckpoint, System.currentTimeMillis() - lastCheckpointTimestamp);
    Checkpointable checkpointable = (Checkpointable) actor;
    if (!checkpointable.shouldCheckpoint(checkpointContext)) {
      return;
    }
    numTasksSinceLastCheckpoint = 0;
    lastCheckpointTimestamp = System.currentTimeMillis();
    UniqueId checkpointId = runtime.rayletClient.prepareCheckpoint(actorId);
    checkpointIds.add(checkpointId);
    if (checkpointIds.size() > NUM_ACTOR_CHECKPOINTS_TO_KEEP) {
      ((Checkpointable) actor).checkpointExpired(actorId, checkpointIds.get(0));
      checkpointIds.remove(0);
    }
    checkpointable.saveCheckpoint(actorId, checkpointId);
  }

  private void maybeLoadCheckpoint(Object actor, ActorId actorId) {
    if (!(actor instanceof Checkpointable)) {
      return;
    }
    if (runtime.getRayConfig().runMode == RunMode.SINGLE_PROCESS) {
      // Actor checkpointing isn't implemented for SINGLE_PROCESS mode yet.
      return;
    }
    numTasksSinceLastCheckpoint = 0;
    lastCheckpointTimestamp = System.currentTimeMillis();
    checkpointIds = new ArrayList<>();
    List<Checkpoint> availableCheckpoints
        = runtime.getGcsClient().getCheckpointsForActor(actorId);
    if (availableCheckpoints.isEmpty()) {
      return;
    }
    UniqueId checkpointId = ((Checkpointable) actor).loadCheckpoint(actorId, availableCheckpoints);
    if (checkpointId != null) {
      boolean checkpointValid = false;
      for (Checkpoint checkpoint : availableCheckpoints) {
        if (checkpoint.checkpointId.equals(checkpointId)) {
          checkpointValid = true;
          break;
        }
      }
      Preconditions.checkArgument(checkpointValid,
          "'loadCheckpoint' must return a checkpoint ID that exists in the "
              + "'availableCheckpoints' list, or null.");
      runtime.rayletClient.notifyActorResumedFromCheckpoint(actorId, checkpointId);
    }
  }
}
