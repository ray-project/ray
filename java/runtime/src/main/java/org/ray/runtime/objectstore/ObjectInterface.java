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

public class ObjectInterface implements BaseObjectInterface {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRayRuntime.class);

  private final long nativeObjectInterface;

  public ObjectInterface(WorkerContext workerContext, RayletClient rayletClient,
                         String storeSocketName) {
    this.nativeObjectInterface =
        nativeCreateObjectInterface(workerContext.getNativeWorkerContext(),
            ((RayletClientImpl) rayletClient).getClient(), storeSocketName);
  }

  @Override
  public ObjectId put(RayObjectProxy obj) {
    return new ObjectId(nativePut(nativeObjectInterface, obj));
  }

  @Override
  public void put(RayObjectProxy obj, ObjectId objectId) {
    try {
      nativePut(nativeObjectInterface, objectId.getBytes(), obj);
    } catch (RayException e) {
      LOGGER.warn(e.getMessage());
    }
  }

  @Override
  public List<RayObjectProxy> get(List<ObjectId> objectIds, long timeoutMs) {
    return nativeGet(nativeObjectInterface, toBinaryList(objectIds), timeoutMs);
  }

  @Override
  public List<Boolean> wait(List<ObjectId> objectIds, int numObjects, long timeoutMs) {
    return nativeWait(nativeObjectInterface, toBinaryList(objectIds), numObjects, timeoutMs);
  }

  @Override
  public void delete(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    nativeDelete(nativeObjectInterface, toBinaryList(objectIds), localOnly, deleteCreatingTasks);
  }

  public void destroy() {
    nativeDestroy(nativeObjectInterface);
  }

  private static List<byte[]> toBinaryList(List<ObjectId> ids) {
    return ids.stream().map(BaseId::getBytes).collect(Collectors.toList());
  }

  private static native long nativeCreateObjectInterface(long nativeObjectInterface,
                                                         long nativeRayletClient,
                                                         String storeSocketName);

  private static native byte[] nativePut(long nativeObjectInterface, RayObjectProxy obj);

  private static native void nativePut(long nativeObjectInterface, byte[] objectId,
                                       RayObjectProxy obj);

  private static native List<RayObjectProxy> nativeGet(long nativeObjectInterface, List<byte[]> ids,
                                                       long timeoutMs);

  private static native List<Boolean> nativeWait(long nativeObjectInterface, List<byte[]> objectIds,
                                                 int numObjects, long timeoutMs);

  private static native void nativeDelete(long nativeObjectInterface, List<byte[]> objectIds,
                                          boolean localOnly, boolean deleteCreatingTasks);

  private static native void nativeDestroy(long nativeObjectInterface);
}
