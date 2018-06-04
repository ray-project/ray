package org.ray.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.UniqueID;
import org.ray.api.returns.MultipleReturns;
import org.ray.spi.model.RayMethod;
import org.ray.spi.model.TaskSpec;
import org.ray.util.exception.TaskExecutionException;
import org.ray.util.logger.RayLog;

/**
 * how to execute a invocation
 */
public class InvocationExecutor {


  public static void execute(TaskSpec task, Pair<ClassLoader, RayMethod> pr)
      throws TaskExecutionException {
    String taskdesc =
        "[" + pr.getRight().fullName + "_" + task.taskId.toString() + " actorId = " + task.actorId
            + "]";
    TaskExecutionException ex = null;

    // switch to current driver's loader
    ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    if (pr.getLeft() != null) {
      Thread.currentThread().setContextClassLoader(pr.getLeft());
    }

    // execute
    try {
      //RayLog.core.debug(task.toString());
      executeInternal(task, pr, taskdesc);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      if (!task.actorId.isNil() && RayRuntime.getInstance().getLocalActor(task.actorId) == null) {
        ex = new TaskExecutionException("Task " + taskdesc + " execution on actor " + task.actorId
            + " failed as the actor is not present ", e);
        RayLog.core.error("Task " + taskdesc + " execution on actor " + task.actorId
            + " failed as the actor is not present ", e);
      } else {
        ex = new TaskExecutionException(
            formatTaskExecutionExceptionMsg(task, pr.getRight().fullName), e);
        RayLog.core.error("Task " + taskdesc + " execution failed ", e);
      }
      RayLog.core.error(e.getMessage());
      RayLog.core.error("task info: \n" + task.toString());
    } catch (Throwable e) {
      ex = new TaskExecutionException(formatTaskExecutionExceptionMsg(task, pr.getRight().fullName),
          e);
      RayLog.core.error("Task " + taskdesc + " execution with unknown error ", e);
      RayLog.core.error(e.getMessage());
    }

    // recover loader
    if (pr.getLeft() != null) {
      Thread.currentThread().setContextClassLoader(oldLoader);
    }

    // set exception as the output results
    if (ex != null) {
      throw ex;
    }
  }

  private static void safePut(UniqueID objectId, Object obj) {
    RayRuntime.getInstance().putRaw(objectId, obj);
  }

  private static void executeInternal(TaskSpec task, Pair<ClassLoader, RayMethod> pr,
      String taskdesc)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Method m = pr.getRight().invokable;
    Map<?, UniqueID> userRayReturnIdMap = null;
    Class<?> returnType = m.getReturnType(); // TODO: not ready for multiple return etc.
    boolean hasMultiReturn = false;
    if(task.returnIds != null && task.returnIds.length > 0) {
      hasMultiReturn = UniqueIdHelper.hasMultipleReturnOrNotFromReturnObjectId(task.returnIds[0]);
    }

    Pair<Object, Object[]> realArgs = ArgumentsBuilder.unwrap(task, m, pr.getLeft());
    if (hasMultiReturn && returnType.equals(Map.class)) {
      //first arg is Map<user_return_id,ray_return_id>
      userRayReturnIdMap = (Map<?, UniqueID>) realArgs.getRight()[0];
      realArgs.getRight()[0] = userRayReturnIdMap.keySet();
    }

    // execute
    Object result;
    if (!UniqueIdHelper.isLambdaFunction(task.functionId)) {
      result = m.invoke(realArgs.getLeft(), realArgs.getRight());
    } else {
      result = m.invoke(realArgs.getLeft(), new Object[]{realArgs.getRight()});
    }

    if(task.returnIds == null || task.returnIds.length == 0) {
      return;
    }
    // set result into storage
    if (MultipleReturns.class.isAssignableFrom(returnType)) {
      MultipleReturns returns = (MultipleReturns) result;
      if (task.returnIds.length != returns.getValues().length) {
        throw new RuntimeException("Mismatched return object count for task " + taskdesc
            + " " + task.returnIds.length + " vs "
            + returns.getValues().length);
      }

      for (int k = 0; k < returns.getValues().length; k++) {
        RayRuntime.getInstance().putRaw(task.returnIds[k], returns.getValues()[k]);
      }
    } else if (hasMultiReturn && returnType.equals(Map.class)) {
      Map<?, ?> returns = (Map<?, ?>) result;
      if (task.returnIds.length != returns.size()) {
        throw new RuntimeException("Mismatched return object count for task " + taskdesc
            + " " + task.returnIds.length + " vs "
            + returns.size());
      }

      for (Entry<?, ?> e : returns.entrySet()) {
        Object userReturnId = e.getKey();
        Object value = e.getValue();
        UniqueID returnId = userRayReturnIdMap.get(userReturnId);
        RayRuntime.getInstance().putRaw(returnId, value);
      }

    } else if (hasMultiReturn && returnType.equals(List.class)) {
      List returns = (List) result;
      if (task.returnIds.length != returns.size()) {
        throw new RuntimeException("Mismatched return object count for task " + taskdesc
            + " " + task.returnIds.length + " vs "
            + returns.size());
      }

      for (int k = 0; k < returns.size(); k++) {
        RayRuntime.getInstance().putRaw(task.returnIds[k], returns.get(k));
      }
    } else {
      RayRuntime.getInstance().putRaw(task.returnIds[0], result);
    }
  }

  private static String formatTaskExecutionExceptionMsg(TaskSpec task, String funcName) {
    return "Execute task " + task.taskId
        + " failed with function name = " + funcName;
  }
}
