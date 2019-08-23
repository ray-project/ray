package org.ray.runtime.task;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.Checkpointable;
import org.ray.api.Checkpointable.Checkpoint;
import org.ray.api.Checkpointable.CheckpointContext;
import org.ray.api.exception.RayTaskException;
import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.functionmanager.RayFunction;
import org.ray.runtime.generated.Common.TaskType;
import org.ray.runtime.object.NativeRayObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The task executor, which executes tasks assigned by raylet continuously.
 */
public final class TaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

  // TODO(hchen): Use the C++ config.
  private static final int NUM_ACTOR_CHECKPOINTS_TO_KEEP = 20;

  protected final AbstractRayRuntime runtime;

  /**
   * The current actor object, if this worker is an actor, otherwise null.
   */
  protected Object currentActor = null;

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

  public TaskExecutor(AbstractRayRuntime runtime) {
    this.runtime = runtime;
  }

  protected List<NativeRayObject> execute(List<String> rayFunctionInfo,
      List<NativeRayObject> argsBytes) {
    JobId jobId = runtime.getWorkerContext().getCurrentJobId();
    TaskType taskType = runtime.getWorkerContext().getCurrentTaskType();
    TaskId taskId = runtime.getWorkerContext().getCurrentTaskId();
    LOGGER.debug("Executing task {}", taskId);

    List<NativeRayObject> returnObjects = new ArrayList<>();
    ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    // Find the executable object.
    RayFunction rayFunction = runtime.getFunctionManager()
        .getFunction(jobId, parseFunctionDescriptor(rayFunctionInfo));
    Preconditions.checkNotNull(rayFunction);
    try {
      Thread.currentThread().setContextClassLoader(rayFunction.classLoader);
      runtime.getWorkerContext().setCurrentClassLoader(rayFunction.classLoader);

      // Get local actor object and arguments.
      Object actor = null;
      if (taskType == TaskType.ACTOR_TASK) {
        if (actorCreationException != null) {
          throw actorCreationException;
        }
        actor = currentActor;

      }
      Object[] args = ArgumentsBuilder.unwrap(runtime.getObjectStore(), argsBytes);
      // Execute the task.
      Object result;
      if (!rayFunction.isConstructor()) {
        result = rayFunction.getMethod().invoke(actor, args);
      } else {
        result = rayFunction.getConstructor().newInstance(args);
      }
      // Set result
      if (taskType != TaskType.ACTOR_CREATION_TASK) {
        if (taskType == TaskType.ACTOR_TASK) {
          // TODO (kfstorm): handle checkpoint in core worker.
          maybeSaveCheckpoint(actor, runtime.getWorkerContext().getCurrentActorId());
        }
        if (rayFunction.hasReturn()) {
          returnObjects.add(runtime.getObjectStore().serialize(result));
        }
      } else {
        // TODO (kfstorm): handle checkpoint in core worker.
        maybeLoadCheckpoint(result, runtime.getWorkerContext().getCurrentActorId());
        currentActor = result;
      }
      LOGGER.debug("Finished executing task {}", taskId);
    } catch (Exception e) {
      LOGGER.error("Error executing task " + taskId, e);
      if (taskType != TaskType.ACTOR_CREATION_TASK) {
        if(rayFunction.hasReturn()) {
          returnObjects.add(runtime.getObjectStore()
              .serialize(new RayTaskException("Error executing task " + taskId, e)));
        }
      } else {
        actorCreationException = e;
      }
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
      runtime.getWorkerContext().setCurrentClassLoader(null);
    }
    return returnObjects;
  }

  private JavaFunctionDescriptor parseFunctionDescriptor(List<String> rayFunctionInfo) {
    Preconditions.checkState(rayFunctionInfo != null && rayFunctionInfo.size() == 3);
    return new JavaFunctionDescriptor(rayFunctionInfo.get(0), rayFunctionInfo.get(1),
        rayFunctionInfo.get(2));
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
    UniqueId checkpointId = runtime.getRayletClient().prepareCheckpoint(actorId);
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
      runtime.getRayletClient().notifyActorResumedFromCheckpoint(actorId, checkpointId);
    }
  }
}
