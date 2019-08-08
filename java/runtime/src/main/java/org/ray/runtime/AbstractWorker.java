package org.ray.runtime;

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
import org.ray.runtime.config.RunMode;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.functionmanager.RayFunction;
import org.ray.runtime.generated.Common.TaskSpec;
import org.ray.runtime.generated.Common.TaskType;
import org.ray.runtime.objectstore.NativeRayObject;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.RayletClient;
import org.ray.runtime.task.ArgumentsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractWorker {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWorker.class);

  // TODO(hchen): Use the C++ config.
  private static final int NUM_ACTOR_CHECKPOINTS_TO_KEEP = 20;

  protected ObjectStoreProxy objectStoreProxy;
  protected TaskInterface taskInterface;
  protected RayletClient rayletClient;
  protected FunctionManager functionManager;
  protected AbstractRayRuntime runtime;
  protected WorkerContext workerContext;

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

  AbstractWorker(AbstractRayRuntime runtime) {
    this.runtime = runtime;
    this.functionManager = runtime.functionManager;
  }

  protected List<NativeRayObject> execute(List<String> rayFunctionInfo,
      List<NativeRayObject> argsBytes) {
    TaskSpec taskSpec = workerContext.getCurrentTask();
    JobId jobId = JobId.fromByteBuffer(taskSpec.getJobId().asReadOnlyByteBuffer());
    TaskType taskType = taskSpec.getType();
    LOGGER.debug("Executing task {}", taskSpec);

    List<NativeRayObject> returnObjects = new ArrayList<>();
    ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Get method
      RayFunction rayFunction = functionManager
          .getFunction(jobId, getJavaFunctionDescriptor(rayFunctionInfo));
      Thread.currentThread().setContextClassLoader(rayFunction.classLoader);
      workerContext.setCurrentClassLoader(rayFunction.classLoader);

      // Get local actor object and arguments.
      Object actor = null;
      if (taskType == TaskType.ACTOR_TASK) {
        if (actorCreationException != null) {
          throw actorCreationException;
        }
        actor = currentActor;

      }
      Object[] args = ArgumentsBuilder.unwrap(this, argsBytes);
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
          ActorId actorId = ActorId
              .fromByteBuffer(taskSpec.getActorTaskSpec().getActorId().asReadOnlyByteBuffer());
          maybeSaveCheckpoint(actor, actorId);
        }
        returnObjects.add(objectStoreProxy.serialize(result));
      } else {
        ActorId actorId = ActorId.fromByteBuffer(
            taskSpec.getActorCreationTaskSpec().getActorId().asReadOnlyByteBuffer());
        maybeLoadCheckpoint(result, actorId);
        currentActor = result;
      }
      LOGGER.debug("Finished executing task {}", TaskId.fromBytes(taskSpec.getTaskId().toByteArray()));
    } catch (Exception e) {
      LOGGER.error("Error executing task " + taskSpec, e);
      if (taskType != TaskType.ACTOR_CREATION_TASK) {
        returnObjects.add(objectStoreProxy
            .serialize(new RayTaskException("Error executing task " + taskSpec, e)));
      } else {
        actorCreationException = e;
      }
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
      workerContext.setCurrentClassLoader(null);
    }
    return returnObjects;
  }

  private JavaFunctionDescriptor getJavaFunctionDescriptor(List<String> rayFunctionInfo) {
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
    UniqueId checkpointId = rayletClient.prepareCheckpoint(actorId);
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
      rayletClient.notifyActorResumedFromCheckpoint(actorId, checkpointId);
    }
  }

  public ObjectStoreProxy getObjectStoreProxy() {
    return objectStoreProxy;
  }

  public TaskInterface getTaskInterface() {
    return taskInterface;
  }

  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  public RayletClient getRayletClient() {
    return rayletClient;
  }
}
