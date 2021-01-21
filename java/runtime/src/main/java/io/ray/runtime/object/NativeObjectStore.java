package io.ray.runtime.object;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.id.BaseId;
import io.ray.api.id.ObjectId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.context.WorkerContext;
import io.ray.runtime.generated.Common.Address;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object store methods for cluster mode. This is a wrapper class for core worker object interface.
 */
public class NativeObjectStore extends ObjectStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeObjectStore.class);

  private final ReadWriteLock shutdownLock;

  public NativeObjectStore(WorkerContext workerContext, ReadWriteLock shutdownLock) {
    super(workerContext);
    this.shutdownLock = shutdownLock;
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
  public List<Boolean> wait(
      List<ObjectId> objectIds, int numObjects, long timeoutMs, boolean fetchLocal) {
    return nativeWait(toBinaryList(objectIds), numObjects, timeoutMs, fetchLocal);
  }

  @Override
  public void delete(List<ObjectId> objectIds, boolean localOnly) {
    nativeDelete(toBinaryList(objectIds), localOnly);
  }

  @Override
  public void addLocalReference(UniqueId workerId, ObjectId objectId) {
    nativeAddLocalReference(workerId.getBytes(), objectId.getBytes());
  }

  @Override
  public void removeLocalReference(UniqueId workerId, ObjectId objectId) {
    Lock readLock = shutdownLock.readLock();
    readLock.lock();
    try {
      nativeRemoveLocalReference(workerId.getBytes(), objectId.getBytes());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public byte[] promoteAndGetOwnershipInfo(ObjectId objectId) {
    return nativePromoteAndGetOwnershipInfo(objectId.getBytes());
  }

  @Override
  public void registerOwnershipInfoAndResolveFuture(
      ObjectId objectId, ObjectId outerObjectId, byte[] ownerAddress) {
    byte[] outer = null;
    if (outerObjectId != null) {
      outer = outerObjectId.getBytes();
    }
    nativeRegisterOwnershipInfoAndResolveFuture(objectId.getBytes(), outer, ownerAddress);
  }

  public Map<ObjectId, long[]> getAllReferenceCounts() {
    Map<ObjectId, long[]> referenceCounts = new HashMap<>();
    for (Map.Entry<byte[], long[]> entry : nativeGetAllReferenceCounts().entrySet()) {
      referenceCounts.put(new ObjectId(entry.getKey()), entry.getValue());
    }
    return referenceCounts;
  }

  @Override
  public Address getOwnerAddress(ObjectId id) {
    try {
      return Address.parseFrom(nativeGetOwnerAddress(id.getBytes()));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<byte[]> toBinaryList(List<ObjectId> ids) {
    return ids.stream().map(BaseId::getBytes).collect(Collectors.toList());
  }

  private static native byte[] nativePut(NativeRayObject obj);

  private static native void nativePut(byte[] objectId, NativeRayObject obj);

  private static native List<NativeRayObject> nativeGet(List<byte[]> ids, long timeoutMs);

  private static native List<Boolean> nativeWait(
      List<byte[]> objectIds, int numObjects, long timeoutMs, boolean fetchLocal);

  private static native void nativeDelete(List<byte[]> objectIds, boolean localOnly);

  private static native void nativeAddLocalReference(byte[] workerId, byte[] objectId);

  private static native void nativeRemoveLocalReference(byte[] workerId, byte[] objectId);

  private static native Map<byte[], long[]> nativeGetAllReferenceCounts();

  private static native byte[] nativeGetOwnerAddress(byte[] objectId);

  private static native byte[] nativePromoteAndGetOwnershipInfo(byte[] objectId);

  private static native void nativeRegisterOwnershipInfoAndResolveFuture(
      byte[] objectId, byte[] outerObjectId, byte[] ownerAddress);
}
