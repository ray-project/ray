package org.ray.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.id.UniqueId;
import org.ray.spi.model.RayMethod;
import org.ray.spi.model.TaskSpec;
import org.ray.util.exception.TaskExecutionException;
import org.ray.util.logger.RayLog;

/**
 * how to execute a invocation.
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
      executeInternal(task, pr);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      if (!task.actorId.isNil()
          && AbstractRayRuntime.getInstance().getLocalActor(task.actorId) == null) {
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

  private static void executeInternal(TaskSpec task, Pair<ClassLoader, RayMethod> pr)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Method m = pr.getRight().invokable;
    Pair<Object, Object[]> realArgs = ArgumentsBuilder.unwrap(task, m, pr.getLeft());

    // execute
    Object result = null;
    try {
      result = m.invoke(realArgs.getLeft(), realArgs.getRight());
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      RayLog.core.error("invoke failed:" + m);
      throw e;
    }

    if (task.returnIds == null || task.returnIds.length == 0) {
      return;
    }
    AbstractRayRuntime.getInstance().put(task.returnIds[0], result);
  }

  private static String formatTaskExecutionExceptionMsg(TaskSpec task, String funcName) {
    return "Execute task " + task.taskId
        + " failed with function name = " + funcName;
  }

  private static void safePut(UniqueId objectId, Object obj) {
    AbstractRayRuntime.getInstance().put(objectId, obj);
  }
}