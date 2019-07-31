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
import org.ray.runtime.RayActorImpl;
import org.ray.runtime.WorkerContext;
import org.ray.runtime.generated.Gcs.ErrorType;
import org.ray.runtime.nativeTypes.NativeRayObject;
import org.ray.runtime.util.Serializer;

/**
 * A class that is used to put/get objects to/from the object store.
 */
public class ObjectStoreProxy {

  private static final byte[] WORKER_EXCEPTION_META = String
      .valueOf(ErrorType.WORKER_DIED.getNumber()).getBytes();
  private static final byte[] ACTOR_EXCEPTION_META = String
      .valueOf(ErrorType.ACTOR_DIED.getNumber()).getBytes();
  private static final byte[] UNRECONSTRUCTABLE_EXCEPTION_META = String
      .valueOf(ErrorType.OBJECT_UNRECONSTRUCTABLE.getNumber()).getBytes();

  private static final byte[] TASK_EXECUTION_EXCEPTION_META = String
      .valueOf(ErrorType.TASK_EXECUTION_EXCEPTION.getNumber()).getBytes();

  private static final byte[] RAW_TYPE_META = "RAW".getBytes();

  private static final byte[] ACTOR_HANDLE_META = "ACTOR_HANDLE".getBytes();

  private final WorkerContext workerContext;

  private final ObjectInterface objectInterface;

  public ObjectStoreProxy(WorkerContext workerContext, ObjectInterface objectInterface) {
    this.workerContext = workerContext;
    this.objectInterface = objectInterface;
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
        Object object = deserialize(dataAndMeta, ids.get(i));
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

      results.add(result);
    }
    return results;
  }

  @SuppressWarnings("unchecked")
  public Object deserialize(NativeRayObject nativeRayObject, ObjectId objectId) {
    byte[] meta = nativeRayObject.metadata;
    byte[] data = nativeRayObject.data;

    // If meta is not null, deserialize the object from meta.
    if (meta != null && meta.length > 0) {
      // If meta is not null, deserialize the object from meta.
      if (Arrays.equals(meta, RAW_TYPE_META)) {
        return data;
      } else if (Arrays.equals(meta, ACTOR_HANDLE_META)) {
        return RayActorImpl.fromBinary(data);
      } else if (Arrays.equals(meta, WORKER_EXCEPTION_META)) {
        return RayWorkerException.INSTANCE;
      } else if (Arrays.equals(meta, ACTOR_EXCEPTION_META)) {
        return RayActorException.INSTANCE;
      } else if (Arrays.equals(meta, UNRECONSTRUCTABLE_EXCEPTION_META)) {
        return new UnreconstructableException(objectId);
      } else if (Arrays.equals(meta, TASK_EXECUTION_EXCEPTION_META)) {
        return Serializer.decode(data, workerContext.getCurrentClassLoader());
      }
      throw new IllegalArgumentException("Unrecognized metadata " + Arrays.toString(meta));
    } else {
      // If data is not null, deserialize the Java object.
      return Serializer.decode(data, workerContext.getCurrentClassLoader());
    }
  }

  /**
   * Serialize an object.
   *
   * @param object The object to serialize.
   * @return The serialized object.
   */
  public NativeRayObject serialize(Object object) {
    if (object instanceof NativeRayObject) {
      return  (NativeRayObject) object;
    } else if (object instanceof byte[]) {
      // If the object is a byte array, skip serializing it and use a special metadata to
      // indicate it's raw binary. So that this object can also be read by Python.
      return new NativeRayObject((byte[]) object, RAW_TYPE_META);
    } else if (object instanceof RayActorImpl) {
      return new NativeRayObject(((RayActorImpl) object).fork().Binary(),
          ACTOR_HANDLE_META);
    } else if (object instanceof RayTaskException) {
      return new NativeRayObject(Serializer.encode(object),
          TASK_EXECUTION_EXCEPTION_META);
    } else {
      return new NativeRayObject(Serializer.encode(object), null);
    }
  }

  /**
   * Serialize and put an object to the object store.
   *
   * @param object The object to put.
   * @return Id of the object.
   */
  public ObjectId put(Object object) {
    return objectInterface.put(serialize(object));
  }

  /**
   * Wait for a list of objects to appear in the object store.
   *
   * @param objectIds IDs of the objects to wait for.
   * @param numObjects Number of objects that should appear.
   * @param timeoutMs Timeout in milliseconds, wait infinitely if it's negative.
   * @return A bitset that indicates each object has appeared or not.
   */
  public List<Boolean> wait(List<ObjectId> objectIds, int numObjects, long timeoutMs) {
    return objectInterface.wait(objectIds, numObjects, timeoutMs);
  }

  /**
   * Delete a list of objects from the object store.
   *
   * @param objectIds IDs of the objects to delete.
   * @param localOnly Whether only delete the objects in local node, or all nodes in the cluster.
   * @param deleteCreatingTasks Whether also delete the tasks that created these objects.
   */
  public void delete(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    objectInterface.delete(objectIds, localOnly, deleteCreatingTasks);
  }

  /**
   * Get the object interface.
   *
   * @return The object interface.
   */
  public ObjectInterface getObjectInterface() {
    return objectInterface;
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
