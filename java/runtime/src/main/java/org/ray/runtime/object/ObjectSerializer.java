package org.ray.runtime.object;

import java.util.Arrays;
import org.ray.api.exception.RayActorException;
import org.ray.api.exception.RayTaskException;
import org.ray.api.exception.RayWorkerException;
import org.ray.api.exception.UnreconstructableException;
import org.ray.api.id.ObjectId;
import org.ray.runtime.generated.Gcs.ErrorType;
import org.ray.runtime.util.Serializer;

/**
 * Serialize to and deserialize from {@link NativeRayObject}. Metadata is generated during
 * serialization and respected during deserialization.
 */
public class ObjectSerializer {

  private static final byte[] WORKER_EXCEPTION_META = String
      .valueOf(ErrorType.WORKER_DIED.getNumber()).getBytes();
  private static final byte[] ACTOR_EXCEPTION_META = String
      .valueOf(ErrorType.ACTOR_DIED.getNumber()).getBytes();
  private static final byte[] UNRECONSTRUCTABLE_EXCEPTION_META = String
      .valueOf(ErrorType.OBJECT_UNRECONSTRUCTABLE.getNumber()).getBytes();

  private static final byte[] TASK_EXECUTION_EXCEPTION_META = String
      .valueOf(ErrorType.TASK_EXECUTION_EXCEPTION.getNumber()).getBytes();

  private static final byte[] RAW_TYPE_META = "RAW".getBytes();

  /**
   * Deserialize an object from an {@link NativeRayObject} instance.
   *
   * @param nativeRayObject The object to deserialize.
   * @param objectId The associated object ID of the object.
   * @param classLoader The classLoader of the object.
   * @return The deserialized object.
   */
  public static Object deserialize(NativeRayObject nativeRayObject, ObjectId objectId,
      ClassLoader classLoader) {
    byte[] meta = nativeRayObject.metadata;
    byte[] data = nativeRayObject.data;

    if (meta != null && meta.length > 0) {
      // If meta is not null, deserialize the object from meta.
      if (Arrays.equals(meta, RAW_TYPE_META)) {
        return data;
      } else if (Arrays.equals(meta, WORKER_EXCEPTION_META)) {
        return RayWorkerException.INSTANCE;
      } else if (Arrays.equals(meta, ACTOR_EXCEPTION_META)) {
        return RayActorException.INSTANCE;
      } else if (Arrays.equals(meta, UNRECONSTRUCTABLE_EXCEPTION_META)) {
        return new UnreconstructableException(objectId);
      } else if (Arrays.equals(meta, TASK_EXECUTION_EXCEPTION_META)) {
        return Serializer.decode(data, classLoader);
      }
      throw new IllegalArgumentException("Unrecognized metadata " + Arrays.toString(meta));
    } else {
      // If data is not null, deserialize the Java object.
      return Serializer.decode(data, classLoader);
    }
  }

  /**
   * Serialize an Java object to an {@link NativeRayObject} instance.
   *
   * @param object The object to serialize.
   * @return The serialized object.
   */
  public static NativeRayObject serialize(Object object) {
    if (object instanceof NativeRayObject) {
      return (NativeRayObject) object;
    } else if (object instanceof byte[]) {
      // If the object is a byte array, skip serializing it and use a special metadata to
      // indicate it's raw binary. So that this object can also be read by Python.
      return new NativeRayObject((byte[]) object, RAW_TYPE_META);
    } else if (object instanceof RayTaskException) {
      return new NativeRayObject(Serializer.encode(object),
          TASK_EXECUTION_EXCEPTION_META);
    } else {
      return new NativeRayObject(Serializer.encode(object), null);
    }
  }
}
