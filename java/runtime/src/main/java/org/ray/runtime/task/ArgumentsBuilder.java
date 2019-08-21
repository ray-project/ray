package org.ray.runtime.task;

import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.object.NativeRayObject;
import org.ray.runtime.object.ObjectStore;
import org.ray.runtime.util.Serializer;

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
  public static List<FunctionArg> wrap(Object[] args, boolean crossLanguage) {
    List<FunctionArg> ret = new ArrayList<>();
    for (Object arg : args) {
      ObjectId id = null;
      byte[] data = null;
      if (arg == null) {
        data = Serializer.encode(null);
      } else if (arg instanceof RayObject) {
        id = ((RayObject) arg).getId();
      } else if (arg instanceof byte[] && crossLanguage) {
        // If the argument is a byte array and will be used by a different language,
        // do not inline this argument. Because the other language doesn't know how
        // to deserialize it.
        id = Ray.put(arg).getId();
      } else {
        byte[] serialized = Serializer.encode(arg);
        if (serialized.length > LARGEST_SIZE_PASS_BY_VALUE) {
          id = ((AbstractRayRuntime) Ray.internal()).getObjectStore()
              .put(new NativeRayObject(serialized, null));
        } else {
          data = serialized;
        }
      }
      if (id != null) {
        ret.add(FunctionArg.passByReference(id));
      } else {
        ret.add(FunctionArg.passByValue(data));
      }
    }
    return ret;
  }

  /**
   * Convert list of NativeRayObject to real function arguments.
   */
  public static Object[] unwrap(ObjectStore objectStore, List<NativeRayObject> args) {
    Object[] realArgs = new Object[args.size()];
    for (int i = 0; i < args.size(); i++) {
      realArgs[i] = objectStore.deserialize(args.get(i), null);
    }
    return realArgs;
  }
}
