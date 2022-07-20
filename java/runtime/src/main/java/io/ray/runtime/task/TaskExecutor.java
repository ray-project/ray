package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayException;
import io.ray.api.exception.RayIntentionalSystemExitException;
import io.ray.api.exception.RayTaskException;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.AbstractRayRuntime;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import io.ray.runtime.functionmanager.RayFunction;
import io.ray.runtime.generated.Common.TaskType;
import io.ray.runtime.object.NativeRayObject;
import io.ray.runtime.object.ObjectSerializer;
import io.ray.runtime.util.NetworkUtil;
import io.ray.runtime.util.SystemUtil;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The task executor, which executes tasks assigned by raylet continuously. */
public abstract class TaskExecutor<T extends TaskExecutor.ActorContext> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

  protected final AbstractRayRuntime runtime;

  private T actorContext = null;

  private final ThreadLocal<RayFunction> localRayFunction = new ThreadLocal<>();

  static class ActorContext {
    /** The current actor object, if this worker is an actor, otherwise null. */
    Object currentActor = null;
  }

  TaskExecutor(AbstractRayRuntime runtime) {
    this.runtime = runtime;
  }

  protected abstract T createActorContext();

  T getActorContext() {
    return actorContext;
  }

  void setActorContext(UniqueId workerId, T actorContext) {
    if (actorContext == null) {
      // ConcurrentHashMap doesn't allow null values. So just return here.
      return;
    }
    this.actorContext = actorContext;
  }

  private RayFunction getRayFunction(List<String> rayFunctionInfo) {
    JobId jobId = runtime.getWorkerContext().getCurrentJobId();
    JavaFunctionDescriptor functionDescriptor = parseFunctionDescriptor(rayFunctionInfo);
    return runtime.getFunctionManager().getFunction(functionDescriptor);
  }

  /** The return value indicates which parameters are ByteBuffer. */
  protected boolean[] checkByteBufferArguments(List<String> rayFunctionInfo) {
    localRayFunction.set(null);
    try {
      localRayFunction.set(getRayFunction(rayFunctionInfo));
    } catch (Throwable e) {
      // Ignore the exception.
      return null;
    }
    Class<?>[] types = localRayFunction.get().executable.getParameterTypes();
    boolean[] results = new boolean[types.length];
    for (int i = 0; i < types.length; i++) {
      results[i] = types[i] == ByteBuffer.class;
    }
    return results;
  }

  private void throwIfDependencyFailed(Object arg) {
    if (arg instanceof RayException) {
      throw (RayException) arg;
    }
  }

  protected List<NativeRayObject> execute(List<String> rayFunctionInfo, List<Object> argsBytes) {
    TaskType taskType = runtime.getWorkerContext().getCurrentTaskType();
    TaskId taskId = runtime.getWorkerContext().getCurrentTaskId();
    LOGGER.debug("Executing task {} {}", taskId, rayFunctionInfo);

    T actorContext = null;
    if (taskType == TaskType.ACTOR_CREATION_TASK) {
      actorContext = createActorContext();
      setActorContext(runtime.getWorkerContext().getCurrentWorkerId(), actorContext);
    } else if (taskType == TaskType.ACTOR_TASK) {
      actorContext = getActorContext();
      Preconditions.checkNotNull(actorContext);
    }

    List<NativeRayObject> returnObjects = new ArrayList<>();
    // Find the executable object.

    RayFunction rayFunction = localRayFunction.get();
    Object[] args = null;
    try {
      // Find the executable object.
      if (rayFunction == null) {
        // Failed to get RayFunction in checkByteBufferArguments. Redo here to throw
        // the exception again.
        rayFunction = getRayFunction(rayFunctionInfo);
      }
      Thread.currentThread().setContextClassLoader(rayFunction.classLoader);

      // Get local actor object and arguments.
      Object actor = null;
      if (taskType == TaskType.ACTOR_TASK) {
        actor = actorContext.currentActor;
      }
      args = ArgumentsBuilder.unwrap(argsBytes, rayFunction.executable.getParameterTypes());
      for (Object arg : args) {
        throwIfDependencyFailed(arg);
      }

      // Execute the task.
      Object result;
      try {
        if (!rayFunction.isConstructor()) {
          result = rayFunction.getMethod().invoke(actor, args);
        } else {
          result = rayFunction.getConstructor().newInstance(args);
        }
      } catch (InvocationTargetException e) {
        if (e.getCause() != null) {
          throw e.getCause();
        } else {
          throw e;
        }
      }

      // Set result
      if (taskType != TaskType.ACTOR_CREATION_TASK) {
        if (rayFunction.hasReturn()) {
          returnObjects.add(ObjectSerializer.serialize(result));
        }
      } else {
        actorContext.currentActor = result;
      }
      LOGGER.debug("Finished executing task {}", taskId);
    } catch (Throwable e) {
      if (e instanceof RayIntentionalSystemExitException) {
        // We don't need to fill the `returnObjects` with an exception metadata
        // because the node manager or the direct actor task submitter will fill
        // the return object with the ACTOR_DIED metadata.
        throw (RayIntentionalSystemExitException) e;
      }

      final List<Class<?>> argTypes =
          args == null
              ? null
              : Arrays.stream(args)
                  .map(arg -> arg == null ? null : arg.getClass())
                  .collect(Collectors.toList());
      LOGGER.error(
          "Failed to execute task {} . rayFunction is {} , argument types are {}",
          taskId,
          rayFunction,
          argTypes,
          e);

      if (taskType != TaskType.ACTOR_CREATION_TASK) {
        boolean hasReturn = rayFunction != null && rayFunction.hasReturn();
        boolean isCrossLanguage = parseFunctionDescriptor(rayFunctionInfo).signature.equals("");
        if (hasReturn || isCrossLanguage) {
          NativeRayObject serializedException;
          try {
            serializedException =
                ObjectSerializer.serialize(
                    new RayTaskException(
                        SystemUtil.pid(),
                        NetworkUtil.getIpAddress(null),
                        "Error executing task " + taskId,
                        e));
          } catch (Exception unserializable) {
            // We should try-catch `ObjectSerializer.serialize` here. Because otherwise if the
            // application-level exception is not serializable. `ObjectSerializer.serialize`
            // will throw an exception and crash the worker.
            // Refer to the case `TaskExceptionTest.java` for more details.
            LOGGER.warn("Failed to serialize the exception to a RayObject.", unserializable);
            serializedException =
                ObjectSerializer.serialize(
                    new RayTaskException(
                        String.format(
                            "Error executing task %s with the exception: %s",
                            taskId, ExceptionUtils.getStackTrace(e))));
          }
          Preconditions.checkNotNull(serializedException);
          returnObjects.add(serializedException);
        } else {
          returnObjects.add(
              ObjectSerializer.serialize(
                  new RayTaskException(
                      SystemUtil.pid(),
                      NetworkUtil.getIpAddress(null),
                      String.format(
                          "Function %s of task %s doesn't exist",
                          String.join(".", rayFunctionInfo), taskId),
                      e)));
        }
      } else {
        throw new RayActorException(SystemUtil.pid(), NetworkUtil.getIpAddress(null), e);
      }
    }
    return returnObjects;
  }

  private JavaFunctionDescriptor parseFunctionDescriptor(List<String> rayFunctionInfo) {
    Preconditions.checkState(rayFunctionInfo != null && rayFunctionInfo.size() == 3);
    return new JavaFunctionDescriptor(
        rayFunctionInfo.get(0), rayFunctionInfo.get(1), rayFunctionInfo.get(2));
  }
}
