package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.Checkpointable;
import org.ray.api.Checkpointable.Checkpoint;
import org.ray.api.Checkpointable.CheckpointContext;
import org.ray.api.exception.RayTaskException;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.functionmanager.RayFunction;
import org.ray.runtime.generated.Common.TaskSpec;
import org.ray.runtime.generated.Common.TaskType;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.nativeTypes.NativeRayObject;
import org.ray.runtime.objectstore.ObjectInterfaceImpl;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.RayletClient;
import org.ray.runtime.raylet.RayletClientImpl;
import org.ray.runtime.task.ArgumentsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The worker, which pulls tasks from raylet and executes them continuously.
 */
public class Worker {

  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  // TODO(hchen): Use the C++ config.
  private static final int NUM_ACTOR_CHECKPOINTS_TO_KEEP = 20;

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

  private final ObjectStoreProxy objectStoreProxy;
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

  Worker(WorkerType workerType, AbstractRayRuntime runtime, FunctionManager functionManager,
      String storeSocket, String rayletSocket, JobId jobId) {
    this.runtime = runtime;
    this.functionManager = functionManager;
    nativeCoreWorkerPointer = nativeInit(workerType.getNumber(), storeSocket, rayletSocket,
        jobId.getBytes());
    Preconditions.checkState(nativeCoreWorkerPointer != 0);
    rayletClient = new RayletClientImpl(nativeCoreWorkerPointer);
    workerContext = new WorkerContext(nativeCoreWorkerPointer);
    objectStoreProxy = new ObjectStoreProxy(workerContext,
        new ObjectInterfaceImpl(nativeCoreWorkerPointer));
    taskInterface = new TaskInterface(nativeCoreWorkerPointer);
  }

  // This method is required by JNI
  private List<NativeRayObject> runTaskCallback(List<String> rayFunctionInfo,
      List<NativeRayObject> argsBytes) {
    TaskSpec taskSpec = workerContext.getCurrentTask();
    JobId jobId = JobId.fromByteBuffer(taskSpec.getJobId().asReadOnlyByteBuffer());
    TaskId taskId = TaskId.fromByteBuffer(taskSpec.getTaskId().asReadOnlyByteBuffer());
    TaskType taskType = taskSpec.getType();
    String taskInfo = taskId + " " + String.join(".", rayFunctionInfo);
    LOGGER.debug("Executing task {}", taskInfo);

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
          UniqueId actorId = UniqueId
              .fromByteBuffer(taskSpec.getActorTaskSpec().getActorId().asReadOnlyByteBuffer());
          maybeSaveCheckpoint(actor, actorId);
        }
        returnObjects.add(objectStoreProxy.serialize(result));
      } else {
        UniqueId actorId = UniqueId.fromByteBuffer(
            taskSpec.getActorCreationTaskSpec().getActorId().asReadOnlyByteBuffer());
        maybeLoadCheckpoint(result, actorId);
        currentActor = result;
      }
      LOGGER.debug("Finished executing task {}", taskInfo);
    } catch (Exception e) {
      LOGGER.error("Error executing task " + taskInfo, e);
      if (taskType != TaskType.ACTOR_CREATION_TASK) {
        returnObjects.add(objectStoreProxy
            .serialize(new RayTaskException("Error executing task " + taskInfo, e)));
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
    nativeRunCoreWorker(nativeCoreWorkerPointer, this);
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

  public void destroy() {
    nativeDestroy(nativeCoreWorkerPointer);
  }

  private static native long nativeInit(int workerMode, String storeSocket,
      String rayletSocket, byte[] jobId);

  private static native void nativeRunCoreWorker(long nativeCoreWorkerPointer, Worker worker);

  private static native void nativeDestroy(long nativeWorkerContextPointer);
}
