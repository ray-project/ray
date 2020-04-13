package io.ray.runtime.object;

import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayTaskException;
import io.ray.api.exception.RayWorkerException;
import io.ray.api.exception.UnreconstructableException;
import io.ray.api.id.ObjectId;
import io.ray.runtime.generated.Gcs.ErrorType;
import io.ray.runtime.serializer.Serializer;
import java.util.Arrays;
import org.apache.commons.lang3.tuple.Pair;

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

  public static final byte[] OBJECT_METADATA_TYPE_CROSS_LANGUAGE = "XLANG".getBytes();
  public static final byte[] OBJECT_METADATA_TYPE_JAVA = "JAVA".getBytes();
  public static final byte[] OBJECT_METADATA_TYPE_PYTHON = "PYTHON".getBytes();
  public static final byte[] OBJECT_METADATA_TYPE_RAW = "RAW".getBytes();

  /**
   * Deserialize an object from an {@link NativeRayObject} instance.
   *
   * @param nativeRayObject The object to deserialize.
   * @param objectId The associated object ID of the object.
   * @return The deserialized object.
   */
  public static Object deserialize(NativeRayObject nativeRayObject, ObjectId objectId,
      Class<?> objectType) {
    byte[] meta = nativeRayObject.metadata;
    byte[] data = nativeRayObject.data;

    if (meta != null && meta.length > 0) {
      // If meta is not null, deserialize the object from meta.
      if (Arrays.equals(meta, OBJECT_METADATA_TYPE_RAW)) {
        return data;
      } else if (Arrays.equals(meta, OBJECT_METADATA_TYPE_CROSS_LANGUAGE) ||
          Arrays.equals(meta, OBJECT_METADATA_TYPE_JAVA)) {
        return Serializer.decode(data, objectType);
      } else if (Arrays.equals(meta, WORKER_EXCEPTION_META)) {
        return new RayWorkerException();
      } else if (Arrays.equals(meta, ACTOR_EXCEPTION_META)) {
        return new RayActorException();
      } else if (Arrays.equals(meta, UNRECONSTRUCTABLE_EXCEPTION_META)) {
        return new UnreconstructableException(objectId);
      } else if (Arrays.equals(meta, TASK_EXECUTION_EXCEPTION_META)) {
        return Serializer.decode(data, objectType);
      } else if (Arrays.equals(meta, OBJECT_METADATA_TYPE_PYTHON)) {
        throw new IllegalArgumentException("Can't deserialize Python object: " + objectId
            .toString());
      }
      throw new IllegalArgumentException("Unrecognized metadata " + Arrays.toString(meta));
    } else {
      // If data is not null, deserialize the Java object.
      return Serializer.decode(data, objectType);
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
      return new NativeRayObject((byte[]) object, OBJECT_METADATA_TYPE_RAW);
    } else if (object instanceof RayTaskException) {
      byte[] serializedBytes = Serializer.encode(object).getLeft();
      return new NativeRayObject(serializedBytes, TASK_EXECUTION_EXCEPTION_META);
    } else {
      Pair<byte[], Boolean> serialized = Serializer.encode(object);
      return new NativeRayObject(serialized.getLeft(), serialized.getRight() ?
          OBJECT_METADATA_TYPE_CROSS_LANGUAGE : OBJECT_METADATA_TYPE_JAVA);
    }
  }
}
