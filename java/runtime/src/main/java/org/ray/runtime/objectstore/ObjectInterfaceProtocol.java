package org.ray.runtime.objectstore;

import java.util.List;
import org.ray.api.id.ObjectId;

public interface ObjectInterfaceProtocol {
  ObjectId put(RayObjectProxy obj);

  void put(RayObjectProxy obj, ObjectId objectId);

  List<RayObjectProxy> get(List<ObjectId> objectIds, long timeoutMs);

  List<Boolean> wait(List<ObjectId> objectIds, int numObjects, long timeoutMs);

  void delete(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks);
}
