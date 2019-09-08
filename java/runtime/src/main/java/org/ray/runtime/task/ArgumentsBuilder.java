package org.ray.runtime.task;

import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.id.ObjectId;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.object.NativeRayObject;
import org.ray.runtime.object.ObjectSerializer;

/**
 * Helper methods to convert arguments from/to objects.
 */
public class ArgumentsBuilder {

  /**
   * If the the size of an argument's serialized data is smaller than this number, the argument will
   * be passed by value. Otherwise it'll be passed by reference.
   */
  private static final int LARGEST_SIZE_PASS_BY_VALUE = 100 * 1024;

  /**
   * Convert real function arguments to task spec arguments.
   */
  public static List<FunctionArg> wrap(Object[] args, boolean isDirectCall) {
    List<FunctionArg> ret = new ArrayList<>();
    for (Object arg : args) {
      ObjectId id = null;
      NativeRayObject value = null;
      if (arg instanceof RayObject) {
        if (isDirectCall) {
          throw new IllegalArgumentException(
              "Passing RayObject to a direct call actor is not supported.");
        }
        id = ((RayObject) arg).getId();
      } else {
        value = ObjectSerializer.serialize(arg);
        if (!isDirectCall && value.data.length > LARGEST_SIZE_PASS_BY_VALUE) {
          RayRuntime runtime = Ray.internal();
          if (runtime instanceof RayMultiWorkerNativeRuntime) {
            runtime = ((RayMultiWorkerNativeRuntime) runtime).getCurrentRuntime();
          }
          id = ((AbstractRayRuntime) runtime).getObjectStore()
              .putRaw(value);
          value = null;
        }
      }
      if (id != null) {
        ret.add(FunctionArg.passByReference(id));
      } else {
        ret.add(FunctionArg.passByValue(value));
      }
    }
    return ret;
  }

  /**
   * Convert list of NativeRayObject to real function arguments.
   */
  public static Object[] unwrap(List<NativeRayObject> args, ClassLoader classLoader) {
    Object[] realArgs = new Object[args.size()];
    for (int i = 0; i < args.size(); i++) {
      realArgs[i] = ObjectSerializer.deserialize(args.get(i), null, classLoader);
    }
    return realArgs;
  }
}
