package org.ray.runtime.task;

import java.util.Arrays;
import java.util.List;
import org.ray.api.RayObject;
import org.ray.api.id.ObjectId;
import org.ray.runtime.Worker;
import org.ray.runtime.proxyTypes.RayObjectProxy;
import org.ray.runtime.util.RayObjectConverter;
import org.ray.runtime.util.Serializer;

public class ArgumentsBuilder {

  /**
   * If the the size of an argument's serialized data is smaller than this number, the argument will
   * be passed by value. Otherwise it'll be passed by reference.
   */
  private static final int LARGEST_SIZE_PASS_BY_VALUE = 100 * 1024;

  /**
   * Convert real function arguments to task spec arguments.
   */
  public static FunctionArg[] wrap(Worker worker, Object[] args,
                                   boolean crossLanguage) {
    FunctionArg[] ret = new FunctionArg[args.length];
    for (int i = 0; i < ret.length; i++) {
      Object arg = args[i];
      ObjectId id = null;
      byte[] data = null;
      if (arg instanceof RayObject) {
        id = ((RayObject) arg).getId();
      } else if (arg instanceof byte[]) {
        if (crossLanguage || ((byte[]) arg).length > LARGEST_SIZE_PASS_BY_VALUE) {
          // If the argument is a byte array and will be used by a different language
          // or it's too large,
          // do not inline this argument. Because the other language doesn't know how
          // to deserialize it.
          id = worker.getObjectInterface().put(arg);
        } else {
          // TODO (kfstorm): support pass by value with metadata
          // Pass by value for byte array must call Serializer.encode(...) to keep type information.
          // We could use RayObjectProxy.data here only after pass by value with metadata is
          // supported.
          data = Serializer.encode(arg);
        }
      } else {
        RayObjectProxy rayObjectProxy =
            worker.getWorkerContext().getRayObjectConverter().toRayObject(arg);
        if (Arrays.equals(rayObjectProxy.metadata, RayObjectConverter.JAVA_OBJECT_META)
            && rayObjectProxy.data.length <= LARGEST_SIZE_PASS_BY_VALUE) {
          data = rayObjectProxy.data;
        } else {
          id = worker.getObjectInterface().putSerialized(rayObjectProxy);
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
   * Convert list of byte array to real function arguments.
   */
  public static Object[] unwrap(Worker worker, List<RayObjectProxy> args) {
    return args.stream().map(arg -> worker.getWorkerContext().getRayObjectConverter().fromRayObject(arg)).toArray();
  }
}
