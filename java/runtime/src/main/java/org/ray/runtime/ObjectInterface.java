package org.ray.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.exception.RayException;
import org.ray.api.id.BaseId;
import org.ray.api.id.ObjectId;
import org.ray.runtime.proxyTypes.RayObjectValueProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectInterface {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRayRuntime.class);

  private final long nativeCoreWorker;
  private final WorkerContext workerContext;

  public ObjectInterface(long nativeCoreWorker, WorkerContext workerContext) {
    this.nativeCoreWorker = nativeCoreWorker;
    this.workerContext = workerContext;
  }

  /**
   * Serialize and put an object to the object store.
   *
   * @param obj The object to put.
   * @return Id of the object.
   */
  public ObjectId put(Object obj) {
    return putInternal(workerContext.getRayObjectValueConverter().toValue(obj));
  }

  /**
   * Put an already serialized object to the object store.
   *
   * @param serializedObject The serialized object to put.
   * @return Id of the object.
   */
  public ObjectId putSerialized(RayObjectValueProxy serializedObject) {
    return putInternal(serializedObject);
  }

  /**
   * Serialize and put an object to the object store.
   *
   * @param objectId Id of the object.
   * @param obj      The object to put.
   */
  public void put(ObjectId objectId, Object obj) {
    putInternal(objectId, workerContext.getRayObjectValueConverter().toValue(obj));
  }

  private ObjectId putInternal(RayObjectValueProxy value) {
    return new ObjectId(put(nativeCoreWorker, value));
  }

  private void putInternal(ObjectId objectId, RayObjectValueProxy value) {
    try {
      put(nativeCoreWorker, objectId.getBytes(), value);
    } catch (RayException e) {
      LOGGER.warn(e.getMessage());
    }
  }

  public <T> List<GetResult<T>> get(List<ObjectId> objectIds, long timeoutMs) {
    List<RayObjectValueProxy> getResults = get(nativeCoreWorker, toBinaryList(objectIds),
        timeoutMs);

    List<GetResult<T>> results = new ArrayList<>();

    for (RayObjectValueProxy getResult : getResults) {
      GetResult<T> result;
      if (getResult != null) {
        Object object =
            workerContext.getRayObjectValueConverter().fromValue(getResult);
        if (object instanceof RayException) {
          // If the object is a `RayException`, it means that an error occurred during task
          // execution.
          result = new GetResult<>(true, null, (RayException) object);
        } else {
          // Otherwise, the object is valid.
          result = new GetResult<>(true, (T) object, null);
        }

        // TODO: release
//        // Release the object from object store..
//        objectStore.get().release(binaryIds[i]);
      } else {
        // If both meta and data are null, the object doesn't exist in object store.
        result = new GetResult<>(false, null, null);
      }

      results.add(result);
    }

    return results;
  }

  public List<Boolean> wait(List<ObjectId> objectIds, int numObjects, long timeoutMs) {
    return wait(nativeCoreWorker, toBinaryList(objectIds), numObjects, timeoutMs);
  }

  public void delete(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    delete(nativeCoreWorker, toBinaryList(objectIds), localOnly, deleteCreatingTasks);
  }

  private static List<byte[]> toBinaryList(List<ObjectId> ids) {
    return ids.stream().map(BaseId::getBytes).collect(Collectors.toList());
  }

  private static native byte[] put(long nativeCoreWorker, RayObjectValueProxy value);

  private static native void put(long nativeCoreWorker, byte[] objectId, RayObjectValueProxy value);

  private static native List<RayObjectValueProxy> get(long nativeCoreWorker, List<byte[]> ids,
                                                      long timeoutMs);

  private static native List<Boolean> wait(long nativeCoreWorker, List<byte[]> objectIds,
                                           int numObjects, long timeoutMs);

  private static native void delete(long nativeCoreWorker, List<byte[]> objectIds, boolean localOnly,
                                    boolean deleteCreatingTasks);

}
