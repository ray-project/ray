package org.ray.runtime.objectstore;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.plasma.ObjectStoreLink.ObjectStoreData;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.ray.api.exception.RayActorException;
import org.ray.api.exception.RayException;
import org.ray.api.exception.RayWorkerException;
import org.ray.api.exception.UnreconstructableException;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.RayDevRuntime;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.generated.ErrorType;
import org.ray.runtime.util.IdUtil;
import org.ray.runtime.util.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that is used to put/get objects to/from the object store.
 */
public class ObjectStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStoreProxy.class);

  private static final byte[] WORKER_EXCEPTION_META = String.valueOf(ErrorType.WORKER_DIED)
      .getBytes();
  private static final byte[] ACTOR_EXCEPTION_META = String.valueOf(ErrorType.ACTOR_DIED)
      .getBytes();
  private static final byte[] UNRECONSTRUCTABLE_EXCEPTION_META = String
      .valueOf(ErrorType.OBJECT_UNRECONSTRUCTABLE).getBytes();

  private static final byte[] RAW_TYPE_META = "RAW".getBytes();

  private final AbstractRayRuntime runtime;

  private static ThreadLocal<ObjectStoreLink> objectStore;

  public ObjectStoreProxy(AbstractRayRuntime runtime, String storeSocketName) {
    this.runtime = runtime;
    objectStore = ThreadLocal.withInitial(() -> {
      if (runtime.getRayConfig().runMode == RunMode.CLUSTER) {
        return new PlasmaClient(storeSocketName, "", 0);
      } else {
        return ((RayDevRuntime) runtime).getObjectStore();
      }
    });
  }

  /**
   * Get an object from the object store.
   *
   * @param id Id of the object.
   * @param timeoutMs Timeout in milliseconds.
   * @param <T> Type of the object.
   * @return The GetResult object.
   */
  public <T> GetResult<T> get(ObjectId id, int timeoutMs) {
    List<GetResult<T>> list = get(ImmutableList.of(id), timeoutMs);
    return list.get(0);
  }

  /**
   * Get a list of objects from the object store.
   *
   * @param ids List of the object ids.
   * @param timeoutMs Timeout in milliseconds.
   * @param <T> Type of these objects.
   * @return A list of GetResult objects.
   */
  public <T> List<GetResult<T>> get(List<ObjectId> ids, int timeoutMs) {
    byte[][] binaryIds = IdUtil.getIdBytes(ids);
    List<ObjectStoreData> dataAndMetaList = objectStore.get().get(binaryIds, timeoutMs);

    List<GetResult<T>> results = new ArrayList<>();
    for (int i = 0; i < dataAndMetaList.size(); i++) {
      byte[] meta = dataAndMetaList.get(i).metadata;
      byte[] data = dataAndMetaList.get(i).data;

      GetResult<T> result;
      if (meta != null) {
        // If meta is not null, deserialize the object from meta.
        result = deserializeFromMeta(meta, data, ids.get(i));
      } else if (data != null) {
        // If data is not null, deserialize the Java object.
        Object object = Serializer.decode(data, runtime.getWorkerContext().getCurrentClassLoader());
        if (object instanceof RayException) {
          // If the object is a `RayException`, it means that an error occurred during task
          // execution.
          result = new GetResult<>(true, null, (RayException) object);
        } else {
          // Otherwise, the object is valid.
          result = new GetResult<>(true, (T) object, null);
        }
      } else {
        // If both meta and data are null, the object doesn't exist in object store.
        result = new GetResult<>(false, null, null);
      }

      if (meta != null || data != null) {
        // Release the object from object store..
        objectStore.get().release(binaryIds[i]);
      }

      results.add(result);
    }
    return results;
  }

  @SuppressWarnings("unchecked")
  private <T> GetResult<T> deserializeFromMeta(byte[] meta, byte[] data, ObjectId objectId) {
    if (Arrays.equals(meta, RAW_TYPE_META)) {
      return (GetResult<T>) new GetResult<>(true, data, null);
    } else if (Arrays.equals(meta, WORKER_EXCEPTION_META)) {
      return new GetResult<>(true, null, RayWorkerException.INSTANCE);
    } else if (Arrays.equals(meta, ACTOR_EXCEPTION_META)) {
      return new GetResult<>(true, null, RayActorException.INSTANCE);
    } else if (Arrays.equals(meta, UNRECONSTRUCTABLE_EXCEPTION_META)) {
      return new GetResult<>(true, null, new UnreconstructableException(objectId));
    }
    throw new IllegalArgumentException("Unrecognized metadata " + Arrays.toString(meta));
  }

  /**
   * Serialize and put an object to the object store.
   *
   * @param id Id of the object.
   * @param object The object to put.
   */
  public void put(ObjectId id, Object object) {
    try {
      if (object instanceof byte[]) {
        // If the object is a byte array, skip serializing it and use a special metadata to
        // indicate it's raw binary. So that this object can also be read by Python.
        objectStore.get().put(id.getBytes(), (byte[]) object, RAW_TYPE_META);
      } else {
        objectStore.get().put(id.getBytes(), Serializer.encode(object), null);
      }
    } catch (DuplicateObjectException e) {
      LOGGER.warn(e.getMessage());
    }
  }

  /**
   * Put an already serialized object to the object store.
   *
   * @param id Id of the object.
   * @param serializedObject The serialized object to put.
   */
  public void putSerialized(ObjectId id, byte[] serializedObject) {
    try {
      objectStore.get().put(id.getBytes(), serializedObject, null);
    } catch (DuplicateObjectException e) {
      LOGGER.warn(e.getMessage());
    }
  }

  /**
   * A class that represents the result of a get operation.
   */
  public static class GetResult<T> {

    /**
     * Whether this object exists in object store.
     */
    public final boolean exists;

    /**
     * The Java object that was fetched and deserialized from the object store. Note, this field
     * only makes sense when @code{exists == true && exception !=null}.
     */
    public final T object;

    /**
     * If this field is not null, it represents the exception that occurred during object's creating
     * task.
     */
    public final RayException exception;

    GetResult(boolean exists, T object, RayException exception) {
      this.exists = exists;
      this.object = object;
      this.exception = exception;
    }
  }
}
