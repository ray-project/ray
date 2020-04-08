package io.ray.runtime.object;

import io.ray.api.id.BaseId;
import io.ray.api.id.ObjectId;
import io.ray.runtime.context.WorkerContext;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object store methods for cluster mode. This is a wrapper class for core worker object interface.
 */
public class NativeObjectStore extends ObjectStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeObjectStore.class);

  public NativeObjectStore(WorkerContext workerContext) {
    super(workerContext);
  }

  @Override
  public ObjectId putRaw(NativeRayObject obj) {
    return new ObjectId(nativePut(obj));
  }

  @Override
  public void putRaw(NativeRayObject obj, ObjectId objectId) {
    nativePut(objectId.getBytes(), obj);
  }

  @Override
  public List<NativeRayObject> getRaw(List<ObjectId> objectIds, long timeoutMs) {
    return nativeGet(toBinaryList(objectIds), timeoutMs);
  }

  @Override
  public List<Boolean> wait(List<ObjectId> objectIds, int numObjects, long timeoutMs) {
    return nativeWait(toBinaryList(objectIds), numObjects, timeoutMs);
  }

  @Override
  public void delete(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    nativeDelete(toBinaryList(objectIds), localOnly, deleteCreatingTasks);
  }

  private static List<byte[]> toBinaryList(List<ObjectId> ids) {
    return ids.stream().map(BaseId::getBytes).collect(Collectors.toList());
  }

  private static native byte[] nativePut(NativeRayObject obj);

  private static native void nativePut(byte[] objectId, NativeRayObject obj);

  private static native List<NativeRayObject> nativeGet(List<byte[]> ids, long timeoutMs);

  private static native List<Boolean> nativeWait(List<byte[]> objectIds, int numObjects,
      long timeoutMs);

  private static native void nativeDelete(List<byte[]> objectIds, boolean localOnly,
      boolean deleteCreatingTasks);
}
