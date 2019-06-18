package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.lang.reflect.Modifier;
import java.util.List;
import org.ray.api.exception.RayTaskException;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.WorkerMode;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.functionmanager.RayFunction;
import org.ray.runtime.task.ArgumentsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker {
  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  private final long nativeCoreWorker;

  private final ObjectInterface objectInterface;
  private final TaskInterface taskInterface;
  private final FunctionManager functionManager;

  /**
   * The current actor object, if this worker is an actor, otherwise null.
   */
  private Object currentActor = null;

  /**
   * The exception that failed the actor creation task, if any.
   */
  private Exception actorCreationException = null;

  private ClassLoader currentClassLoader;

  Worker(WorkerMode workerMode, FunctionManager functionManager, String storeSocket,
         String rayletSocket, UniqueId driverId) {
    this.functionManager = functionManager;
    nativeCoreWorker = createCoreWorker(workerMode.value(), storeSocket, rayletSocket,
        driverId.getBytes());
    objectInterface = new ObjectInterface(nativeCoreWorker);
    taskInterface = new TaskInterface(nativeCoreWorker);
  }

  private void runTaskCallback(List<String> rayFunctionInfo, List<byte[]> argsBytes,
                               byte[] taskIdBytes, int numReturns) {
    TaskId taskId = new TaskId(taskIdBytes);
    String taskInfo = taskId + " " + String.join(".", rayFunctionInfo);
    LOGGER.debug("Executing task {}", taskInfo);
    Preconditions.checkState(numReturns == 1);

    ObjectId returnId = new ObjectId(getTaskReturnId(taskIdBytes, 1));
    ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Get method
      RayFunction rayFunction = functionManager
          .getFunction(getCurrentDriverId(), getJavaFunctionDescriptor(rayFunctionInfo));
      Thread.currentThread().setContextClassLoader(rayFunction.classLoader);
      currentClassLoader = rayFunction.classLoader;
      boolean isActorCreationTask = rayFunction.isConstructor();
      boolean isActorTask =
          !isActorCreationTask && !Modifier.isStatic(rayFunction.getMethod().getModifiers());

//      if (isActorCreationTask) {
//        currentActorId = new UniqueId(returnId.getBytes());
//      }

      // Get local actor object and arguments.
      Object actor = null;
      if (isActorTask) {
        // TODO: check is in core worker?
        // Preconditions.checkState(spec.actorId.equals(currentActorId));
        if (actorCreationException != null) {
          throw actorCreationException;
        }
        actor = currentActor;

      }
      Object[] args = ArgumentsBuilder.unwrap(argsBytes, rayFunction.classLoader);
      // Execute the task.
      Object result;
      if (!rayFunction.isConstructor()) {
        result = rayFunction.getMethod().invoke(actor, args);
      } else {
        result = rayFunction.getConstructor().newInstance(args);
      }
      // Set result
      if (!isActorCreationTask) {
        if (isActorTask) {
          // TODO
          // maybeSaveCheckpoint(actor, spec.actorId);
        }
        objectInterface.put(returnId, result);
      } else {
        // TODO
        // maybeLoadCheckpoint(result, new UniqueId(returnId.getBytes()));
        currentActor = result;
      }
      LOGGER.debug("Finished executing task {}", taskInfo);
    } catch (Exception e) {
      LOGGER.error("Error executing task " + taskInfo, e);
      // TODO
//      if (!isActorCreationTask) {
      objectInterface.put(returnId, new RayTaskException("Error executing task " + taskId,
          e));
//      } else {
//        actorCreationException = e;
//      }
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
      currentClassLoader = null;
    }
  }

  private JavaFunctionDescriptor getJavaFunctionDescriptor(List<String> rayFunctionInfo) {
    Preconditions.checkState(rayFunctionInfo != null && rayFunctionInfo.size() == 3);
    return new JavaFunctionDescriptor(rayFunctionInfo.get(0), rayFunctionInfo.get(1),
        rayFunctionInfo.get(2));
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

  public ClassLoader getCurrentClassLoader() {
    return currentClassLoader;
  }

  public UniqueId getCurrentDriverId() {
    return new UniqueId(getCurrentDriverId(nativeCoreWorker));
  }

  private static native long createCoreWorker(int workerMode, String storeSocket,
                                              String rayletSocket, byte[] driverId);

  private static native void runCoreWorker(long nativeCoreWorker, Worker worker);

  private static native byte[] getCurrentDriverId(long nativeCoreWorker);

  private static native byte[] getTaskReturnId(byte[] taskId, long returnIndex);
}
