package org.ray.runtime.objectstore;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.ray.api.exception.RayActorException;
import org.ray.api.exception.RayException;
import org.ray.api.exception.RayTaskException;
import org.ray.api.exception.RayWorkerException;
import org.ray.api.exception.UnreconstructableException;
import org.ray.api.id.ObjectId;
import org.ray.runtime.WorkerContext;
import org.ray.runtime.generated.Gcs.ErrorType;
import org.ray.runtime.util.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that is used to put/get objects to/from the object store.
 */
public class ObjectStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStoreProxy.class);

  private static final byte[] WORKER_EXCEPTION_META = String
      .valueOf(ErrorType.WORKER_DIED.getNumber()).getBytes();
  private static final byte[] ACTOR_EXCEPTION_META = String
      .valueOf(ErrorType.ACTOR_DIED.getNumber()).getBytes();
  private static final byte[] UNRECONSTRUCTABLE_EXCEPTION_META = String
      .valueOf(ErrorType.OBJECT_UNRECONSTRUCTABLE.getNumber()).getBytes();

  private static final byte[] TASK_EXECUTION_EXCEPTION_META = String
      .valueOf(ErrorType.TASK_EXECUTION_EXCEPTION.getNumber()).getBytes();

  private static final byte[] RAW_TYPE_META = "RAW".getBytes();

  private final WorkerContext workerContext;

  private final ObjectInterface objectInterface;

  public ObjectStoreProxy(WorkerContext workerContext, ObjectInterface objectInterface) {
    this.workerContext = workerContext;
    this.objectInterface = objectInterface;
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
    List<NativeRayObject> dataAndMetaList = objectInterface.get(ids, timeoutMs);

    List<GetResult<T>> results = new ArrayList<>();
    for (int i = 0; i < dataAndMetaList.size(); i++) {
      NativeRayObject dataAndMeta = dataAndMetaList.get(i);
      GetResult<T> result;
      if (dataAndMeta != null) {
        byte[] meta = dataAndMeta.metadata;
        byte[] data = dataAndMeta.data;
        if (meta != null && meta.length > 0) {
          // If meta is not null, deserialize the object from meta.
          result = deserializeFromMeta(meta, data,
              workerContext.getCurrentClassLoader(), ids.get(i));
        } else {
          // If data is not null, deserialize the Java object.
          Object object = Serializer.decode(data, workerContext.getCurrentClassLoader());
          if (object instanceof RayException) {
            // If the object is a `RayException`, it means that an error occurred during task
            // execution.
            result = new GetResult<>(true, null, (RayException) object);
          } else {
            // Otherwise, the object is valid.
            result = new GetResult<>(true, (T) object, null);
          }
        }
      } else {
        // If both meta and data are null, the object doesn't exist in object store.
        result = new GetResult<>(false, null, null);
      }

      results.add(result);
    }
    return results;
  }

  @SuppressWarnings("unchecked")
  private <T> GetResult<T> deserializeFromMeta(byte[] meta, byte[] data,
      ClassLoader classLoader, ObjectId objectId) {
    if (Arrays.equals(meta, RAW_TYPE_META)) {
      return (GetResult<T>) new GetResult<>(true, data, null);
    } else if (Arrays.equals(meta, WORKER_EXCEPTION_META)) {
      return new GetResult<>(true, null, RayWorkerException.INSTANCE);
    } else if (Arrays.equals(meta, ACTOR_EXCEPTION_META)) {
      return new GetResult<>(true, null, RayActorException.INSTANCE);
    } else if (Arrays.equals(meta, UNRECONSTRUCTABLE_EXCEPTION_META)) {
      return new GetResult<>(true, null, new UnreconstructableException(objectId));
    } else if (Arrays.equals(meta, TASK_EXECUTION_EXCEPTION_META)) {
      return new GetResult<>(true, null, Serializer.decode(data, classLoader));
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
    if (object instanceof byte[]) {
      // If the object is a byte array, skip serializing it and use a special metadata to
      // indicate it's raw binary. So that this object can also be read by Python.
      objectInterface.put(new NativeRayObject((byte[]) object, RAW_TYPE_META), id);
    } else if (object instanceof RayTaskException) {
      objectInterface.put(new NativeRayObject(Serializer.encode(object), TASK_EXECUTION_EXCEPTION_META), id);
    } else {
      objectInterface.put(new NativeRayObject(Serializer.encode(object), null), id);
    }
  }

  /**
   * Put an already serialized object to the object store.
   *
   * @param id Id of the object.
   * @param serializedObject The serialized object to put.
   */
  public void putSerialized(ObjectId id, byte[] serializedObject) {
    objectInterface.put(new NativeRayObject(serializedObject, null), id);
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
