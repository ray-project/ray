package io.ray.runtime.object;

import com.google.common.base.Preconditions;
import io.ray.api.exception.RayTimeoutException;
import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.context.WorkerContext;
import io.ray.runtime.generated.Common.Address;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Object store methods for local mode. */
public class LocalModeObjectStore extends ObjectStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalModeObjectStore.class);

  private static final int GET_CHECK_INTERVAL_MS = 1;

  private final Map<ObjectId, NativeRayObject> pool = new ConcurrentHashMap<>();
  private final List<Consumer<ObjectId>> objectPutCallbacks = new ArrayList<>();

  public LocalModeObjectStore(WorkerContext workerContext) {
    super(workerContext);
  }

  public void addObjectPutCallback(Consumer<ObjectId> callback) {
    this.objectPutCallbacks.add(callback);
  }

  public boolean isObjectReady(ObjectId id) {
    return pool.containsKey(id);
  }

  @Override
  public ObjectId putRaw(NativeRayObject obj) {
    ObjectId objectId = ObjectId.fromRandom();
    putRaw(obj, objectId);
    return objectId;
  }

  @Override
  public ObjectId putRaw(NativeRayObject obj, ActorId ownerActorId) {
    throw new UnsupportedOperationException(
        "Assigning owner in Ray:put is not implemented in local mode");
  }

  @Override
  public void putRaw(NativeRayObject obj, ObjectId objectId) {
    Preconditions.checkNotNull(obj);
    Preconditions.checkNotNull(objectId);
    pool.putIfAbsent(objectId, obj);
    for (Consumer<ObjectId> callback : objectPutCallbacks) {
      callback.accept(objectId);
    }
  }

  @Override
  public List<NativeRayObject> getRaw(List<ObjectId> objectIds, long timeoutMs) {
    waitInternal(objectIds, objectIds.size(), timeoutMs);
    if (timeoutMs >= 0 && objectIds.stream().filter(pool::containsKey).count() < objectIds.size()) {
      throw new RayTimeoutException("Get timed out: some object(s) not ready.");
    }
    return objectIds.stream().map(pool::get).collect(Collectors.toList());
  }

  @Override
  public List<Boolean> wait(
      List<ObjectId> objectIds, int numObjects, long timeoutMs, boolean fetchLocal) {
    waitInternal(objectIds, numObjects, timeoutMs);
    return objectIds.stream().map(pool::containsKey).collect(Collectors.toList());
  }

  private void waitInternal(List<ObjectId> objectIds, int numObjects, long timeoutMs) {
    int ready = 0;
    long remainingTime = timeoutMs;
    boolean firstCheck = true;
    while (ready < numObjects && (timeoutMs < 0 || remainingTime > 0)) {
      if (!firstCheck) {
        long sleepTime =
            timeoutMs < 0 ? GET_CHECK_INTERVAL_MS : Math.min(remainingTime, GET_CHECK_INTERVAL_MS);
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOGGER.warn("Got InterruptedException while sleeping.");
        }
        remainingTime -= sleepTime;
      }
      ready = 0;
      for (ObjectId objectId : objectIds) {
        if (pool.containsKey(objectId)) {
          ready += 1;
        }
      }
      firstCheck = false;
    }
  }

  @Override
  public void delete(List<ObjectId> objectIds, boolean localOnly) {
    for (ObjectId objectId : objectIds) {
      pool.remove(objectId);
    }
  }

  @Override
  public void addLocalReference(UniqueId workerId, ObjectId objectId) {}

  @Override
  public void removeLocalReference(UniqueId workerId, ObjectId objectId) {}

  @Override
  public Address getOwnerAddress(ObjectId id) {
    return Address.getDefaultInstance();
  }

  @Override
  public byte[] getOwnershipInfo(ObjectId objectId) {
    return new byte[0];
  }

  @Override
  public void registerOwnershipInfoAndResolveFuture(
      ObjectId objectId, ObjectId outerObjectId, byte[] ownerAddress) {}
}
