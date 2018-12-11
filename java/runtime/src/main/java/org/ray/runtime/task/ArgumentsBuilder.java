package org.ray.runtime.task;

import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.id.UniqueId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.util.Serializer;

public class ArgumentsBuilder {

  /**
   * If the the size of an argument's serialized data is smaller than this number,
   * the argument will be passed by value. Otherwise it'll be passed by reference.
   */
  private static final int LARGEST_SIZE_PASS_BY_VALUE = 100 * 1024;


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
      } else if (arg instanceof byte[]) {
        id = Ray.put(arg).getId();
      } else {
        byte[] serialized = Serializer.encode(arg);
        if (serialized.length > LARGEST_SIZE_PASS_BY_VALUE) {
          id = ((AbstractRayRuntime)Ray.internal()).putSerialized(serialized).getId();
        } else {
          data = serialized;
        }
      }
      if (id != null) {
        ret[i] = FunctionArg.passByReference(id);
      } else {
        ret[i] = FunctionArg.passByValue(data);
      }
    }
    return ret;
  }

  /**
   * Convert task spec arguments to real function arguments.
   */
  public static Object[] unwrap(TaskSpec task, ClassLoader classLoader) {
    Object[] realArgs = new Object[task.args.length];
    List<UniqueId> idsToFetch = new ArrayList<>();
    List<Integer> indices = new ArrayList<>();
    for (int i = 0; i < task.args.length; i++) {
      FunctionArg arg = task.args[i];
      if (arg.id != null) {
        // pass by reference
        idsToFetch.add(arg.id);
        indices.add(i);
      } else {
        // pass by value
        realArgs[i] = Serializer.decode(arg.data, classLoader);
      }
    }
    List<Object> objects = Ray.get(idsToFetch);
    for (int i = 0; i < objects.size(); i++) {
      realArgs[indices.get(i)] = objects.get(i);
    }
    return realArgs;
  }
}
