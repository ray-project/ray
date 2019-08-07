package org.ray.runtime.objectstore;

import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.exception.RayException;
import org.ray.api.id.BaseId;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.WorkerContext;
import org.ray.runtime.raylet.RayletClient;
import org.ray.runtime.raylet.RayletClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a wrapper class for core worker object interface.
 */
public class ObjectInterfaceImpl implements ObjectInterface {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRayRuntime.class);

  /**
   * The native pointer of core worker object interface.
   */
  private final long nativeObjectInterfacePointer;

  public ObjectInterfaceImpl(WorkerContext workerContext, RayletClient rayletClient,
      String storeSocketName) {
    this.nativeObjectInterfacePointer =
        nativeCreateObjectInterface(workerContext.getNativeWorkerContext(),
            ((RayletClientImpl) rayletClient).getClient(), storeSocketName);
  }

  @Override
  public ObjectId put(NativeRayObject obj) {
    return new ObjectId(nativePut(nativeObjectInterfacePointer, obj));
  }

  @Override
  public void put(NativeRayObject obj, ObjectId objectId) {
    try {
      nativePut(nativeObjectInterfacePointer, objectId.getBytes(), obj);
    } catch (RayException e) {
      LOGGER.warn(e.getMessage());
    }
  }

  @Override
  public List<NativeRayObject> get(List<ObjectId> objectIds, long timeoutMs) {
    return nativeGet(nativeObjectInterfacePointer, toBinaryList(objectIds), timeoutMs);
  }

  @Override
  public List<Boolean> wait(List<ObjectId> objectIds, int numObjects, long timeoutMs) {
    return nativeWait(nativeObjectInterfacePointer, toBinaryList(objectIds), numObjects, timeoutMs);
  }

  @Override
  public void delete(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    nativeDelete(nativeObjectInterfacePointer,
        toBinaryList(objectIds), localOnly, deleteCreatingTasks);
  }

  public void destroy() {
    nativeDestroy(nativeObjectInterfacePointer);
  }

  private static List<byte[]> toBinaryList(List<ObjectId> ids) {
    return ids.stream().map(BaseId::getBytes).collect(Collectors.toList());
  }

  private static native long nativeCreateObjectInterface(long nativeObjectInterface,
      long nativeRayletClient,
      String storeSocketName);

  private static native byte[] nativePut(long nativeObjectInterface, NativeRayObject obj);

  private static native void nativePut(long nativeObjectInterface, byte[] objectId,
      NativeRayObject obj);

  private static native List<NativeRayObject> nativeGet(long nativeObjectInterface,
      List<byte[]> ids,
      long timeoutMs);

  private static native List<Boolean> nativeWait(long nativeObjectInterface, List<byte[]> objectIds,
      int numObjects, long timeoutMs);

  private static native void nativeDelete(long nativeObjectInterface, List<byte[]> objectIds,
      boolean localOnly, boolean deleteCreatingTasks);

  private static native void nativeDestroy(long nativeObjectInterface);
}
