package org.ray.runtime.objectstore;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
   * @param <T> Type of the object.
   * @return The GetResult object.
   */
  public <T> T get(ObjectId id) {
    List<T> list = get(ImmutableList.of(id));
    return list.get(0);
  }

  /**
   * Get a list of objects from the object store.
   *
   * @param ids List of the object ids.
   * @param <T> Type of these objects.
   * @return A list of GetResult objects.
   */
  @SuppressWarnings("unchecked")
  public <T> List<T> get(List<ObjectId> ids) {
    // Pass -1 as timeout to wait until all objects are available in object store.
    List<NativeRayObject> dataAndMetaList = objectInterface.get(ids, -1);

    List<T> results = new ArrayList<>();
    for (int i = 0; i < dataAndMetaList.size(); i++) {
      NativeRayObject dataAndMeta = dataAndMetaList.get(i);
      Object object = null;
      if (dataAndMeta != null) {
        byte[] meta = dataAndMeta.metadata;
        byte[] data = dataAndMeta.data;
        if (meta != null && meta.length > 0) {
          // If meta is not null, deserialize the object from meta.
          object = deserializeFromMeta(meta, data,
              workerContext.getCurrentClassLoader(), ids.get(i));
        } else {
          // If data is not null, deserialize the Java object.
          object = Serializer.decode(data, workerContext.getCurrentClassLoader());
        }
        if (object instanceof RayException) {
          // If the object is a `RayException`, it means that an error occurred during task
          // execution.
          throw (RayException) object;
        }
      }

      results.add((T) object);
    }
    // This check must be placed after the throw exception statement.
    // Because if there was any exception, The get operation would return early
    // and wouldn't wait until all objects exist.
    Preconditions.checkState(dataAndMetaList.stream().allMatch(Objects::nonNull));
    return results;
  }

  private Object deserializeFromMeta(byte[] meta, byte[] data,
      ClassLoader classLoader, ObjectId objectId) {
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
      objectInterface
          .put(new NativeRayObject(Serializer.encode(object), TASK_EXECUTION_EXCEPTION_META), id);
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

  public ObjectInterface getObjectInterface() {
    return objectInterface;
  }
}
