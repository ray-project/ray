package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.Checkpointable;
import org.ray.api.Checkpointable.Checkpoint;
import org.ray.api.Checkpointable.CheckpointContext;
import org.ray.api.exception.RayTaskException;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.functionmanager.RayFunction;
import org.ray.runtime.proxyTypes.RayObjectProxy;
import org.ray.runtime.raylet.RayletClient;
import org.ray.runtime.raylet.RayletClientImpl;
import org.ray.runtime.task.ArgumentsBuilder;
import org.ray.runtime.task.TaskInfo;
import org.ray.runtime.task.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker {
  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  // TODO(hchen): Use the C++ config.
  private static final int NUM_ACTOR_CHECKPOINTS_TO_KEEP = 20;

  private final long nativeCoreWorker;

  private final ObjectInterface objectInterface;
  private final TaskInterface taskInterface;
  private RayletClient rayletClient;
  private final FunctionManager functionManager;
  private final AbstractRayRuntime runtime;
  private WorkerContext workerContext;

  /**
   * The current actor object, if this worker is an actor, otherwise null.
   */
  private Object currentActor = null;

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

  Worker(AbstractRayRuntime runtime, FunctionManager functionManager,
         String storeSocket, String rayletSocket, UniqueId jobId) {
    this.runtime = runtime;
    this.functionManager = functionManager;
    nativeCoreWorker = createCoreWorker(runtime.getRayConfig().workerMode.value(), storeSocket, rayletSocket,
        jobId.getBytes());
    workerContext = new WorkerContext(nativeCoreWorker);
    objectInterface = new ObjectInterface(nativeCoreWorker, workerContext);
    taskInterface = new TaskInterface(nativeCoreWorker);
    rayletClient = new RayletClientImpl(nativeCoreWorker);
  }

  // This method is required by JNI
  private void runTaskCallback(List<String> rayFunctionInfo, List<RayObjectProxy> argsBytes,
                               byte[] taskIdBytes, byte[] jobIdBytes, int jniTaskType,
                               int numReturns) {
    TaskId taskId = new TaskId(taskIdBytes);
    UniqueId jobId = new UniqueId(jobIdBytes);
    TaskType taskType = TaskType.fromInteger(jniTaskType);
    workerContext.setCurrentTask(new TaskInfo(taskId, jobId, taskType));
    String taskInfo = taskId + " " + String.join(".", rayFunctionInfo);
    LOGGER.debug("Executing task {}", taskInfo);
//    Preconditions.checkState(numReturns == 1);

    ObjectId returnId = new ObjectId(getTaskReturnId(taskIdBytes, 1));
    ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Get method
      RayFunction rayFunction = functionManager
          .getFunction(workerContext.getCurrentJobId(), getJavaFunctionDescriptor(rayFunctionInfo));
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
          maybeSaveCheckpoint(actor, workerContext.getCurrentActorId());
        }
        objectInterface.put(returnId, result);
      } else {
        maybeLoadCheckpoint(result, new UniqueId(returnId.getBytes()));
        currentActor = result;
      }
      LOGGER.debug("Finished executing task {}", taskInfo);
    } catch (Exception e) {
      LOGGER.error("Error executing task " + taskInfo, e);
      if (taskType != TaskType.ACTOR_CREATION_TASK) {
        objectInterface.put(returnId, new RayTaskException("Error executing task " + taskInfo, e));
      } else {
        actorCreationException = e;
      }
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
      workerContext.setCurrentClassLoader(null);
      workerContext.setCurrentTask(null);
    }
  }

  private JavaFunctionDescriptor getJavaFunctionDescriptor(List<String> rayFunctionInfo) {
    Preconditions.checkState(rayFunctionInfo != null && rayFunctionInfo.size() == 3);
    return new JavaFunctionDescriptor(rayFunctionInfo.get(0), rayFunctionInfo.get(1),
        rayFunctionInfo.get(2));
  }

  private void maybeSaveCheckpoint(Object actor, UniqueId actorId) {
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

  private void maybeLoadCheckpoint(Object actor, UniqueId actorId) {
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

  public void loop() {
    runCoreWorker(nativeCoreWorker, this);
  }

  public ObjectInterface getObjectInterface() {
    return objectInterface;
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

  private static native long createCoreWorker(int workerMode, String storeSocket,
                                              String rayletSocket, byte[] jobId);

  private static native void runCoreWorker(long nativeCoreWorker, Worker worker);

  private static native byte[] getTaskReturnId(byte[] taskId, long returnIndex);
}
