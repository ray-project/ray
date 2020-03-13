package org.ray.runtime.task;

import java.util.ArrayList;
import java.util.List;

import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.id.ObjectId;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.generated.Common.Language;
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
   * This dummy type is also defined in signature.py. Please keep it synced.
   */
  private static final NativeRayObject PYTHON_DUMMY_TYPE = ObjectSerializer
      .serialize("__RAY_DUMMY__".getBytes());

  /**
   * Convert real function arguments to task spec arguments.
   */
  public static List<FunctionArg> wrap(Object[] args, Language language) {
    List<FunctionArg> ret = new ArrayList<>();
    for (Object arg : args) {
      ObjectId id = null;
      NativeRayObject value = null;
      if (arg instanceof RayObject) {
        id = ((RayObject) arg).getId();
      } else {
        value = ObjectSerializer.serialize(arg);
        if (value.data.length > LARGEST_SIZE_PASS_BY_VALUE) {
          RayRuntime runtime = Ray.internal();
          if (runtime instanceof RayMultiWorkerNativeRuntime) {
            runtime = ((RayMultiWorkerNativeRuntime) runtime).getCurrentRuntime();
          }
          id = ((AbstractRayRuntime) runtime).getObjectStore()
            .putRaw(value);
          value = null;
        }
      }
      if (language == Language.PYTHON) {
        ret.add(FunctionArg.passByValue(PYTHON_DUMMY_TYPE));
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
