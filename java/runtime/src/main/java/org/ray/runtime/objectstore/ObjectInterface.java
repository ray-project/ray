package org.ray.runtime.objectstore;

import java.util.List;
import org.ray.api.id.ObjectId;

/**
 * The interface that contains all worker methods that are related to object store.
 */
public interface ObjectInterface {

  /**
   * Put an object into object store.
   *
   * @param obj The ray object.
   * @return Generated ID of the object.
   */
  ObjectId put(NativeRayObject obj);

  /**
   * Put an object with specified ID into object store.
   *
   * @param obj The ray object.
   * @param objectId Object ID specified by user.
   */
  void put(NativeRayObject obj, ObjectId objectId);

  /**
   * Get a list of objects from the object store.
   *
   * @param objectIds IDs of the objects to get.
   * @param timeoutMs Timeout in milliseconds, wait infinitely if it's negative.
   * @return Result list of objects data.
   */
  List<NativeRayObject> get(List<ObjectId> objectIds, long timeoutMs);

  /**
   * Wait for a list of objects to appear in the object store.
   *
   * @param objectIds IDs of the objects to wait for.
   * @param numObjects Number of objects that should appear.
   * @param timeoutMs Timeout in milliseconds, wait infinitely if it's negative.
   * @return A bitset that indicates each object has appeared or not.
   */
  List<Boolean> wait(List<ObjectId> objectIds, int numObjects, long timeoutMs);

  /**
   * Delete a list of objects from the object store.
   *
   * @param objectIds IDs of the objects to delete.
   * @param localOnly Whether only delete the objects in local node, or all nodes in the cluster.
   * @param deleteCreatingTasks Whether also delete the tasks that created these objects.
   */
  void delete(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks);
}
