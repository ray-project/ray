package org.ray.runtime.task;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.exception.RayTaskException;
import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.functionmanager.RayFunction;
import org.ray.runtime.generated.Common.TaskType;
import org.ray.runtime.object.NativeRayObject;
import org.ray.runtime.object.ObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The task executor, which executes tasks assigned by raylet continuously.
 */
public abstract class TaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

  protected final AbstractRayRuntime runtime;

  /**
   * The current actor object, if this worker is an actor, otherwise null.
   */
  protected Object currentActor = null;

  /**
   * The exception that failed the actor creation task, if any.
   */
  private Exception actorCreationException = null;

  protected TaskExecutor(AbstractRayRuntime runtime) {
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
      Object[] args = ArgumentsBuilder.unwrap(argsBytes, rayFunction.classLoader);
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
          returnObjects.add(ObjectSerializer.serialize(result));
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
        if (rayFunction.hasReturn()) {
          returnObjects.add(ObjectSerializer
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

  protected abstract void maybeSaveCheckpoint(Object actor, ActorId actorId);

  protected abstract void maybeLoadCheckpoint(Object actor, ActorId actorId);
}
