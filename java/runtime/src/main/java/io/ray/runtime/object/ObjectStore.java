package io.ray.runtime.object;

import com.google.common.base.Preconditions;
import io.ray.api.ObjectRef;
import io.ray.api.WaitResult;
import io.ray.api.exception.RayException;
import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.runtime.context.WorkerContext;
import io.ray.runtime.generated.Common.Address;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** A class that is used to put/get objects to/from the object store. */
public abstract class ObjectStore {

  private final WorkerContext workerContext;

  public ObjectStore(WorkerContext workerContext) {
    this.workerContext = workerContext;
  }

  /**
   * Put a raw object into object store.
   *
   * @param obj The ray object.
   * @return Generated ID of the object.
   */
  public abstract ObjectId putRaw(NativeRayObject obj);

  /**
   * Put a raw object into object store, and assign its ownership to the actor identified by
   * ownerActorId.
   *
   * @param obj The ray object.
   * @param ownerActorId The id of the actor to assign ownership.
   * @return Generated ID of the object.
   */
  public abstract ObjectId putRaw(NativeRayObject obj, ActorId ownerActorId);

  /**
   * Put a raw object with specified ID into object store.
   *
   * @param obj The ray object.
   * @param objectId Object ID specified by user.
   */
  public abstract void putRaw(NativeRayObject obj, ObjectId objectId);

  /**
   * Serialize and put an object to the object store.
   *
   * @param object The object to put.
   * @return Id of the object.
   */
  public ObjectId put(Object object) {
    if (object instanceof NativeRayObject) {
      throw new IllegalArgumentException(
          "Trying to put a NativeRayObject. Please use putRaw instead.");
    }
    return putRaw(ObjectSerializer.serialize(object));
  }

  /**
   * Serialize and put an object to the object store, and assign its ownership to the actor
   * identified by ownerActorId.
   *
   * @param object The object to put.
   * @param ownerActorId The id of the actor to assign ownership.
   * @return Id of the object.
   */
  public ObjectId put(Object object, ActorId ownerActorId) {
    if (object instanceof NativeRayObject) {
      throw new IllegalArgumentException(
          "Trying to put a NativeRayObject. Please use putRaw instead.");
    }
    return putRaw(ObjectSerializer.serialize(object), ownerActorId);
  }

  /**
   * Serialize and put an object to the object store, with the given object id.
   *
   * <p>This method is only used for testing.
   *
   * @param object The object to put.
   * @param objectId Object id.
   */
  public void put(Object object, ObjectId objectId) {
    if (object instanceof NativeRayObject) {
      throw new IllegalArgumentException(
          "Trying to put a NativeRayObject. Please use putRaw instead.");
    }
    putRaw(ObjectSerializer.serialize(object), objectId);
  }

  /**
   * Get a list of raw objects from the object store.
   *
   * @param objectIds IDs of the objects to get.
   * @param timeoutMs Timeout in milliseconds, wait infinitely if it's negative.
   * @return Result list of objects data.
   * @throws RayTimeoutException If it's timeout to get the object.
   */
  public abstract List<NativeRayObject> getRaw(List<ObjectId> objectIds, long timeoutMs);

  /**
   * Get a list of objects from the object store.
   *
   * @param ids List of the object ids.
   * @param <T> Type of these objects.
   * @return A list of GetResult objects.
   */
  @SuppressWarnings("unchecked")
  public <T> List<T> get(List<ObjectId> ids, Class<?> elementType) {
    return get(ids, elementType, -1);
  }

  /**
   * Get a list of objects from the object store.
   *
   * @param ids List of the object ids.
   * @param <T> Type of these objects.
   * @param timeoutMs The maximum amount of time in seconds to wait before returning.
   * @return A list of GetResult objects.
   * @throws RayTimeoutException If it's timeout to get the object.
   */
  @SuppressWarnings("unchecked")
  public <T> List<T> get(List<ObjectId> ids, Class<?> elementType, long timeoutMs) {
    List<NativeRayObject> dataAndMetaList = getRaw(ids, timeoutMs);

    List<T> results = new ArrayList<>();
    for (int i = 0; i < dataAndMetaList.size(); i++) {
      NativeRayObject dataAndMeta = dataAndMetaList.get(i);
      Object object = null;
      if (dataAndMeta != null) {
        try {
          ObjectSerializer.setOuterObjectId(ids.get(i));
          object = ObjectSerializer.deserialize(dataAndMeta, ids.get(i), elementType);
        } finally {
          ObjectSerializer.resetOuterObjectId();
        }
      }
      if (object instanceof RayException) {
        // If the object is a `RayException`, it means that an error occurred during task
        // execution.
        throw (RayException) object;
      }
      results.add((T) object);
    }
    // This check must be placed after the throw exception statement.
    // Because if there was any exception, The get operation would return early
    // and wouldn't wait until all objects exist.
    Preconditions.checkState(dataAndMetaList.stream().allMatch(Objects::nonNull));
    return results;
  }

  /**
   * Wait for a list of RayObjects to be available, until specified number of objects are ready, or
   * specified timeout has passed.
   *
   * @param objectIds IDs of the objects to wait for.
   * @param numObjects Number of objects that should appear.
   * @param timeoutMs Timeout in milliseconds, wait infinitely if it's negative.
   * @param fetchLocal If true, wait for the object to be downloaded onto the local node before
   *     returning it as ready. If false, ray.wait() will not trigger fetching of objects to the
   *     local node and will return immediately once the object is available anywhere in the
   *     cluster.
   * @return A bitset that indicates each object has appeared or not.
   */
  public abstract List<Boolean> wait(
      List<ObjectId> objectIds, int numObjects, long timeoutMs, boolean fetchLocal);

  /**
   * Wait for a list of RayObjects to be available, until specified number of objects are ready, or
   * specified timeout has passed.
   *
   * @param waitList A list of object references to wait for.
   * @param numReturns The number of objects that should be returned.
   * @param timeoutMs The maximum time in milliseconds to wait before returning.
   * @param fetchLocal If true, wait for the object to be downloaded onto the local node before
   *     returning it as ready. If false, ray.wait() will not trigger fetching of objects to the
   *     local node and will return immediately once the object is available anywhere in the
   *     cluster.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public <T> WaitResult<T> wait(
      List<ObjectRef<T>> waitList, int numReturns, int timeoutMs, boolean fetchLocal) {
    Preconditions.checkNotNull(waitList);
    if (waitList.isEmpty()) {
      return new WaitResult<>(Collections.emptyList(), Collections.emptyList());
    }

    List<ObjectId> ids =
        waitList.stream().map(ref -> ((ObjectRefImpl<?>) ref).getId()).collect(Collectors.toList());

    List<Boolean> ready = wait(ids, numReturns, timeoutMs, fetchLocal);
    List<ObjectRef<T>> readyList = new ArrayList<>();
    List<ObjectRef<T>> unreadyList = new ArrayList<>();

    for (int i = 0; i < ready.size(); i++) {
      if (ready.get(i)) {
        readyList.add(waitList.get(i));
      } else {
        unreadyList.add(waitList.get(i));
      }
    }

    return new WaitResult<>(readyList, unreadyList);
  }

  /**
   * Delete a list of objects from the object store.
   *
   * @param objectIds IDs of the objects to delete.
   * @param localOnly Whether only delete the objects in local node, or all nodes in the cluster.
   */
  public abstract void delete(List<ObjectId> objectIds, boolean localOnly);

  /**
   * Increase the local reference count for this object ID.
   *
   * @param objectId The object ID to increase the reference count for.
   */
  public abstract void addLocalReference(ObjectId objectId);

  /**
   * Decrease the reference count for this object ID.
   *
   * @param objectId The object ID to decrease the reference count for.
   */
  public abstract void removeLocalReference(ObjectId objectId);

  public abstract Address getOwnerAddress(ObjectId id);

  /**
   * Get the ownership info.
   *
   * @param objectId The ID of the object to promote
   * @return the serialized ownership address
   */
  public abstract byte[] getOwnershipInfo(ObjectId objectId);

  /**
   * Add a reference to an ObjectID that will deserialized. This will also start the process to
   * resolve the future. Specifically, we will periodically contact the owner, until we learn that
   * the object has been created or the owner is no longer reachable. This will then unblock any
   * Gets or submissions of tasks dependent on the object.
   *
   * @param objectId The object ID to deserialize.
   * @param outerObjectId The object ID that contained objectId, if any. This may be nil if the
   *     object ID was inlined directly in a task spec or if it was passed out-of-band by the
   *     application (deserialized from a byte string).
   * @param ownerAddress The address of the object's owner.
   */
  public abstract void registerOwnershipInfoAndResolveFuture(
      ObjectId objectId, ObjectId outerObjectId, byte[] ownerAddress);
}
