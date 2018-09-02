package org.ray.core;

import java.lang.reflect.Method;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.id.UniqueId;
import org.ray.spi.model.FunctionArg;
import org.ray.spi.model.TaskSpec;

/**
 * arguments wrap and unwrap.
 */
public class ArgumentsBuilder {

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static FunctionArg[] wrap(Object[] args) {
    FunctionArg[] ret = new FunctionArg[args.length];
    for (int i = 0; i < ret.length; i++) {
      Object arg = args[i];
      UniqueId id = null;
      byte[] data = null;
      if (arg == null) {
        data = Serializer.encode(null);
      } else if (arg instanceof RayActor) {
        data = Serializer.encode(arg);
      } else if (arg instanceof RayObject) {
        id = ((RayObject) arg).getId();
      } else if (checkSimpleValue(arg)) {
        data = Serializer.encode(arg);
      } else {
        RayObject obj = Ray.put(arg);
        id = obj.getId();
      }
      ret[i] = new FunctionArg(id, data);
    }
    return ret;
  }

  private static boolean checkSimpleValue(Object o) {
    return true;//TODO I think Ray don't want to pass big parameter
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Pair<Object, Object[]> unwrap(TaskSpec task, Method m, ClassLoader classLoader) {
    // the last arg is className
    Object[] realArgs = new Object[task.args.length - 1];
    for (int i = 0; i < task.args.length - 1; i++) {
      FunctionArg arg = task.args[i];
      if (arg.id == null) {
        // pass by value
        Object obj = Serializer.decode(arg.data, classLoader);
        realArgs[i] = obj;
      } else if (arg.data == null) {
        // pass by reference
        realArgs[i] = Ray.get(arg.id);
      }
    }
    Object actor = task.actorId.isNil()
        ? null : AbstractRayRuntime.getInstance().getLocalActor(task.actorId);
    return Pair.of(actor, realArgs);
  }
}
