package org.ray.runtime.util;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.id.UniqueId;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.task.TaskSpec;

public class ArgumentsBuilder {

  private static boolean checkSimpleValue(Object o) {
    // TODO(raulchen): implement this.
    return true;
  }

  /**
   * Convert real function arguments to task spec arguments.
   */
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

  /**
   * Convert task spec arguments to real function arguments.
   */
  public static Object[] unwrap(TaskSpec task, ClassLoader classLoader) {
    // Ignore the last arg, which is the class name
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
    return realArgs;
  }
}
