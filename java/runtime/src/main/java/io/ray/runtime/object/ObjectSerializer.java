package io.ray.runtime.object;

import com.google.common.primitives.Bytes;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.id.ObjectId;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.exception.RayTaskException;
import io.ray.runtime.exception.RayWorkerException;
import io.ray.runtime.exception.UnreconstructableException;
import io.ray.runtime.generated.Common.ErrorType;
import io.ray.runtime.serializer.Serializer;
import io.ray.runtime.util.IdUtil;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Serialize to and deserialize from {@link NativeRayObject}. Metadata is generated during
 * serialization and respected during deserialization.
 */
public class ObjectSerializer {

  private static final byte[] WORKER_EXCEPTION_META =
      String.valueOf(ErrorType.WORKER_DIED.getNumber()).getBytes();
  private static final byte[] ACTOR_EXCEPTION_META =
      String.valueOf(ErrorType.ACTOR_DIED.getNumber()).getBytes();
  private static final byte[] UNRECONSTRUCTABLE_EXCEPTION_META =
      String.valueOf(ErrorType.OBJECT_UNRECONSTRUCTABLE.getNumber()).getBytes();
  private static final byte[] TASK_EXECUTION_EXCEPTION_META =
      String.valueOf(ErrorType.TASK_EXECUTION_EXCEPTION.getNumber()).getBytes();

  public static final byte[] OBJECT_METADATA_TYPE_CROSS_LANGUAGE = "XLANG".getBytes();
  public static final byte[] OBJECT_METADATA_TYPE_JAVA = "JAVA".getBytes();
  public static final byte[] OBJECT_METADATA_TYPE_PYTHON = "PYTHON".getBytes();
  public static final byte[] OBJECT_METADATA_TYPE_RAW = "RAW".getBytes();
  // A constant used as object metadata to indicate the object is an actor handle.
  // This value should be synchronized with the Python definition in ray_constants.py
  // TODO(fyrestone): Serialize the ActorHandle via the custom type feature of XLANG.
  public static final byte[] OBJECT_METADATA_TYPE_ACTOR_HANDLE = "ACTOR_HANDLE".getBytes();

  // When an outer object is being serialized, the nested ObjectRefs are all
  // serialized and the writeExternal method of the nested ObjectRefs are
  // executed. So after the outer object is serialized, the containedObjectIds
  // field will contain all the nested object IDs.
  static ThreadLocal<Set<ObjectId>> containedObjectIds = ThreadLocal.withInitial(HashSet::new);

  static ThreadLocal<ObjectId> outerObjectId = ThreadLocal.withInitial(() -> null);

  /**
   * Deserialize an object from an {@link NativeRayObject} instance.
   *
   * @param nativeRayObject The object to deserialize.
   * @param objectId The associated object ID of the object.
   * @return The deserialized object.
   */
  public static Object deserialize(
      NativeRayObject nativeRayObject, ObjectId objectId, Class<?> objectType) {
    byte[] meta = nativeRayObject.metadata;
    byte[] data = nativeRayObject.data;

    if (meta != null && meta.length > 0) {
      // If meta is not null, deserialize the object from meta.
      if (Bytes.indexOf(meta, OBJECT_METADATA_TYPE_RAW) == 0) {
        if (objectType == ByteBuffer.class) {
          return ByteBuffer.wrap(data);
        }
        return data;
      } else if (Bytes.indexOf(meta, OBJECT_METADATA_TYPE_CROSS_LANGUAGE) == 0
          || Bytes.indexOf(meta, OBJECT_METADATA_TYPE_JAVA) == 0) {
        return Serializer.decode(data, objectType);
      } else if (Bytes.indexOf(meta, WORKER_EXCEPTION_META) == 0) {
        return new RayWorkerException();
      } else if (Bytes.indexOf(meta, ACTOR_EXCEPTION_META) == 0) {
        return new RayActorException(IdUtil.getActorIdFromObjectId(objectId));
      } else if (Bytes.indexOf(meta, UNRECONSTRUCTABLE_EXCEPTION_META) == 0) {
        return new UnreconstructableException(objectId);
      } else if (Bytes.indexOf(meta, TASK_EXECUTION_EXCEPTION_META) == 0) {
        // Serialization logic of task execution exception: an instance of
        // `io.ray.runtime.exception.RayTaskException`
        //    -> a `RayException` protobuf message
        //    -> protobuf-serialized bytes
        //    -> MessagePack-serialized bytes.
        // So here the `data` variable is MessagePack-serialized bytes, and the `serialized`
        // variable is protobuf-serialized bytes. They are not the same.
        byte[] serialized = Serializer.decode(data, byte[].class);
        try {
          return RayTaskException.fromBytes(serialized);
        } catch (InvalidProtocolBufferException e) {
          throw new IllegalArgumentException(
              "Can't deserialize RayTaskException object: " + objectId.toString());
        }
      } else if (Bytes.indexOf(meta, OBJECT_METADATA_TYPE_ACTOR_HANDLE) == 0) {
        byte[] serialized = Serializer.decode(data, byte[].class);
        return NativeActorHandle.fromBytes(serialized);
      } else if (Bytes.indexOf(meta, OBJECT_METADATA_TYPE_PYTHON) == 0) {
        throw new IllegalArgumentException(
            "Can't deserialize Python object: " + objectId.toString());
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
    } else if (object instanceof ByteBuffer) {
      // Serialize ByteBuffer to raw bytes.
      ByteBuffer buffer = (ByteBuffer) object;
      byte[] bytes;
      if (buffer.hasArray()) {
        bytes = buffer.array();
      } else {
        bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
      }
      return new NativeRayObject(bytes, OBJECT_METADATA_TYPE_RAW);
    } else if (object instanceof RayTaskException) {
      RayTaskException taskException = (RayTaskException) object;
      byte[] serializedBytes = Serializer.encode(taskException.toBytes()).getLeft();
      // serializedBytes is MessagePack serialized bytes
      // taskException.toBytes() is protobuf serialized bytes
      // Only OBJECT_METADATA_TYPE_RAW is raw bytes,
      // any other type should be the MessagePack serialized bytes.
      return new NativeRayObject(serializedBytes, TASK_EXECUTION_EXCEPTION_META);
    } else if (object instanceof NativeActorHandle) {
      NativeActorHandle actorHandle = (NativeActorHandle) object;
      byte[] serializedBytes = Serializer.encode(actorHandle.toBytes()).getLeft();
      // serializedBytes is MessagePack serialized bytes
      // Only OBJECT_METADATA_TYPE_RAW is raw bytes,
      // any other type should be the MessagePack serialized bytes.
      return new NativeRayObject(serializedBytes, OBJECT_METADATA_TYPE_ACTOR_HANDLE);
    } else {
      try {
        Pair<byte[], Boolean> serialized = Serializer.encode(object);
        NativeRayObject nativeRayObject =
            new NativeRayObject(
                serialized.getLeft(),
                serialized.getRight()
                    ? OBJECT_METADATA_TYPE_CROSS_LANGUAGE
                    : OBJECT_METADATA_TYPE_JAVA);
        nativeRayObject.setContainedObjectIds(getAndClearContainedObjectIds());
        return nativeRayObject;
      } catch (Exception e) {
        // Clear `containedObjectIds`.
        getAndClearContainedObjectIds();
        throw e;
      }
    }
  }

  static void addContainedObjectId(ObjectId objectId) {
    containedObjectIds.get().add(objectId);
  }

  private static List<ObjectId> getAndClearContainedObjectIds() {
    List<ObjectId> ids = new ArrayList<>(containedObjectIds.get());
    containedObjectIds.get().clear();
    return ids;
  }

  static void setOuterObjectId(ObjectId objectId) {
    outerObjectId.set(objectId);
  }

  static ObjectId getOuterObjectId() {
    return outerObjectId.get();
  }

  static void resetOuterObjectId() {
    outerObjectId.set(null);
  }
}
