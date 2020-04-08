package io.ray.runtime.task;

import io.ray.api.Ray;
import io.ray.api.RayObject;
import io.ray.api.id.ObjectId;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.object.NativeRayObject;
import io.ray.runtime.object.ObjectSerializer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        if (language != Language.JAVA) {
          boolean isCrossData =
              Arrays.equals(value.metadata, ObjectSerializer.OBJECT_METADATA_TYPE_CROSS_LANGUAGE) ||
                  Arrays.equals(value.metadata, ObjectSerializer.OBJECT_METADATA_TYPE_RAW);
          if (!isCrossData) {
            throw new IllegalArgumentException(String.format("Can't transfer %s data to %s",
                Arrays.toString(value.metadata), language.getValueDescriptor().getName()));
          }
        }
        if (value.data.length > LARGEST_SIZE_PASS_BY_VALUE) {
          id = ((RayRuntimeInternal) Ray.internal()).getObjectStore()
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
  public static Object[] unwrap(List<NativeRayObject> args, Class<?>[] types) {
    Object[] realArgs = new Object[args.size()];
    for (int i = 0; i < args.size(); i++) {
      realArgs[i] = ObjectSerializer.deserialize(args.get(i), null, types[i]);
    }
    return realArgs;
  }
}
