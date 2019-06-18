package org.ray.runtime;

import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.id.ObjectId;
import org.ray.runtime.util.Serializer;

public class ObjectInterface {
  private final long nativeCoreWorker;

  public ObjectInterface(long nativeCoreWorker) {
    this.nativeCoreWorker = nativeCoreWorker;
  }

  public ObjectId put(Object obj) {
    byte[] binary = obj instanceof byte[] ? (byte[]) obj : Serializer.encode(obj);
    return new ObjectId(put(nativeCoreWorker, binary));
  }

  public ObjectId putSerialized(byte[] binary) {
    return new ObjectId(put(nativeCoreWorker, binary));
  }

  public void put(ObjectId objectId, Object obj) {
//    TODO: how to handle byte array with python?
//    // If the object is a byte array, skip serializing it and use a special metadata to
//    // indicate it's raw binary. So that this object can also be read by Python.
//    objectStore.get().put(id.getBytes(), (byte[]) object, RAW_TYPE_META);
    byte[] binary = obj instanceof byte[] ? (byte[]) obj : Serializer.encode(obj);
    putSerialized(objectId, binary);
  }

  public void putSerialized(ObjectId objectId, byte[] binary) {
//    try {
    put(nativeCoreWorker, objectId.getBytes(), binary);
//    TODO: how to handle duplicated object ID error?
//    } catch (DuplicateObjectException e) {
//      LOGGER.warn(e.getMessage());
//    }
  }

  public List<byte[]> get(List<ObjectId> objectIds, long timeoutMs) {
    return get(nativeCoreWorker, toBinaryList(objectIds), timeoutMs);
  }

  public List<Boolean> wait(List<ObjectId> objectIds, int numObjects, long timeoutMs) {
    return wait(nativeCoreWorker, toBinaryList(objectIds), numObjects, timeoutMs);
  }

  public void delete(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    delete(nativeCoreWorker, toBinaryList(objectIds), localOnly, deleteCreatingTasks);
  }

  private static List<byte[]> toBinaryList(List<ObjectId> ids) {
    return ids.stream().map(id -> id.getBytes()).collect(Collectors.toList());
  }

  private static native byte[] put(long nativeCoreWorker, byte[] binary);

  private static native void put(long nativeCoreWorker, byte[] objectId, byte[] binary);

  private static native List<byte[]> get(long nativeCoreWorker, List<byte[]> ids, long timeoutMs);

  private static native List<Boolean> wait(long nativeCoreWorker, List<byte[]> objectIds,
                                       int numObjects, long timeoutMs);

  private static native void delete(long nativeCoreWorker, List<byte[]> objectIds, boolean localOnly,
                                    boolean deleteCreatingTasks);
}
