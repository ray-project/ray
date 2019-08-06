package org.ray.runtime.objectstore;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.ray.api.id.ObjectId;
import org.ray.runtime.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockObjectInterface implements ObjectInterface {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockObjectInterface.class);

  private static final int GET_CHECK_INTERVAL_MS = 100;

  private final Map<ObjectId, NativeRayObject> pool = new ConcurrentHashMap<>();
  private final List<Consumer<ObjectId>> objectPutCallbacks = new ArrayList<>();
  private final WorkerContext workerContext;

  public MockObjectInterface(WorkerContext workerContext) {
    this.workerContext = workerContext;
  }

  public void addObjectPutCallback(Consumer<ObjectId> callback) {
    this.objectPutCallbacks.add(callback);
  }

  public boolean isObjectReady(ObjectId id) {
    return pool.containsKey(id);
  }

  @Override
  public ObjectId put(NativeRayObject obj) {
    ObjectId objectId = ObjectId.forPut(workerContext.getCurrentTaskId(),
        workerContext.nextPutIndex());
    put(obj, objectId);
    return objectId;
  }

  @Override
  public void put(NativeRayObject obj, ObjectId objectId) {
    Preconditions.checkNotNull(obj);
    Preconditions.checkNotNull(objectId);
    pool.putIfAbsent(objectId, obj);
    for (Consumer<ObjectId> callback : objectPutCallbacks) {
      callback.accept(objectId);
    }
  }

  @Override
  public List<NativeRayObject> get(List<ObjectId> objectIds, long timeoutMs) {
    waitInternal(objectIds, objectIds.size(), timeoutMs);
    return objectIds.stream().map(pool::get).collect(Collectors.toList());
  }

  @Override
  public List<Boolean> wait(List<ObjectId> objectIds, int numObjects, long timeoutMs) {
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
  public void delete(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    for (ObjectId objectId : objectIds) {
      pool.remove(objectId);
    }
  }
}
