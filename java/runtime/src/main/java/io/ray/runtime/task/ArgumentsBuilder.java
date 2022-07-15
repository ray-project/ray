package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import io.ray.runtime.AbstractRayRuntime;
import io.ray.runtime.generated.Common.Address;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.object.NativeRayObject;
import io.ray.runtime.object.ObjectRefImpl;
import io.ray.runtime.object.ObjectSerializer;
import io.ray.runtime.util.SystemConfig;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Helper methods to convert arguments from/to objects. */
public class ArgumentsBuilder {

  /**
   * If the the size of an argument's serialized data is smaller than this number, the argument will
   * be passed by value. Otherwise it'll be passed by reference.
   */
  public static final long LARGEST_SIZE_PASS_BY_VALUE =
      ((Double) SystemConfig.get("max_direct_call_object_size")).longValue();

  /** This dummy type is also defined in signature.py. Please keep it synced. */
  private static final NativeRayObject PYTHON_DUMMY_TYPE =
      ObjectSerializer.serialize("__RAY_DUMMY__".getBytes());

  /** Convert real function arguments to task spec arguments. */
  public static List<FunctionArg> wrap(Object[] args, Language language) {
    List<FunctionArg> ret = new ArrayList<>();
    for (Object arg : args) {
      ObjectId id = null;
      Address address = null;
      NativeRayObject value = null;
      if (arg instanceof ObjectRef) {
        Preconditions.checkState(arg instanceof ObjectRefImpl);
        id = ((ObjectRefImpl<?>) arg).getId();
        address = ((AbstractRayRuntime) Ray.internal()).getObjectStore().getOwnerAddress(id);
      } else {
        value = ObjectSerializer.serialize(arg);
        if (language != Language.JAVA) {
          boolean isCrossData =
              Bytes.indexOf(value.metadata, ObjectSerializer.OBJECT_METADATA_TYPE_CROSS_LANGUAGE)
                      == 0
                  || Bytes.indexOf(value.metadata, ObjectSerializer.OBJECT_METADATA_TYPE_RAW) == 0
                  || Bytes.indexOf(
                          value.metadata, ObjectSerializer.OBJECT_METADATA_TYPE_ACTOR_HANDLE)
                      == 0;
          if (!isCrossData) {
            throw new IllegalArgumentException(
                String.format(
                    "Can't transfer %s data to %s",
                    Arrays.toString(value.metadata), language.getValueDescriptor().getName()));
          }
        }
        if (value.data.length > LARGEST_SIZE_PASS_BY_VALUE) {
          id = ((AbstractRayRuntime) Ray.internal()).getObjectStore().putRaw(value);
          address = ((AbstractRayRuntime) Ray.internal()).getWorkerContext().getRpcAddress();
          value = null;
        }
      }
      if (language == Language.PYTHON) {
        ret.add(FunctionArg.passByValue(PYTHON_DUMMY_TYPE));
      }
      if (id != null) {
        ret.add(FunctionArg.passByReference(id, address));
      } else {
        ret.add(FunctionArg.passByValue(value));
      }
    }
    return ret;
  }

  /** Convert list of NativeRayObject/ByteBuffer to real function arguments. */
  public static Object[] unwrap(List<Object> args, Class<?>[] types) {
    Object[] realArgs = new Object[args.size()];
    for (int i = 0; i < args.size(); i++) {
      Object arg = args.get(i);
      Preconditions.checkState(arg instanceof ByteBuffer || arg instanceof NativeRayObject);
      if (arg instanceof ByteBuffer) {
        Preconditions.checkState(types[i] == ByteBuffer.class);
        realArgs[i] = arg;
      } else {
        realArgs[i] = ObjectSerializer.deserialize((NativeRayObject) arg, null, types[i]);
      }
    }
    return realArgs;
  }
}
