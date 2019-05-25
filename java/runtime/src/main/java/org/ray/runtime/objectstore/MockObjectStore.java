package org.ray.runtime.objectstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.arrow.plasma.ObjectStoreLink;
import org.ray.api.id.ObjectId;
import org.ray.runtime.RayDevRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mock implementation of {@code org.ray.spi.ObjectStoreLink}, which use Map to store data.
 */
public class MockObjectStore implements ObjectStoreLink {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockObjectStore.class);

  private static final int GET_CHECK_INTERVAL_MS = 100;

  private final RayDevRuntime runtime;
  private final Map<ObjectId, byte[]> data = new ConcurrentHashMap<>();
  private final Map<ObjectId, byte[]> metadata = new ConcurrentHashMap<>();
  private final List<Consumer<ObjectId>> objectPutCallbacks;

  public MockObjectStore(RayDevRuntime runtime) {
    this.runtime = runtime;
    this.objectPutCallbacks = new ArrayList<>();
  }

  public void addObjectPutCallback(Consumer<ObjectId> callback) {
    this.objectPutCallbacks.add(callback);
  }

  @Override
  public void put(byte[] objectId, byte[] value, byte[] metadataValue) {
    if (objectId == null || objectId.length == 0 || value == null) {
      LOGGER
          .error("{} cannot put null: {}, {}", logPrefix(), objectId, Arrays.toString(value));
      System.exit(-1);
    }
    ObjectId id = new ObjectId(objectId);
    data.put(id, value);
    if (metadataValue != null) {
      metadata.put(id, metadataValue);
    }
    for (Consumer<ObjectId> callback : objectPutCallbacks) {
      callback.accept(id);
    }
  }

  @Override
  public byte[] get(byte[] objectId, int timeoutMs, boolean isMetadata) {
    return get(new byte[][] {objectId}, timeoutMs, isMetadata).get(0);
  }

  @Override
  public List<byte[]> get(byte[][] objectIds, int timeoutMs, boolean isMetadata) {
    return get(objectIds, timeoutMs)
            .stream()
            .map(data -> isMetadata ? data.metadata : data.data)
            .collect(Collectors.toList());
  }

  @Override
  public List<ObjectStoreData> get(byte[][] objectIds, int timeoutMs) {
    int ready = 0;
    int remainingTime = timeoutMs;
    boolean firstCheck = true;
    while (ready < objectIds.length && remainingTime > 0) {
      if (!firstCheck) {
        int sleepTime = Math.min(remainingTime, GET_CHECK_INTERVAL_MS);
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOGGER.warn("Got InterruptedException while sleeping.");
        }
        remainingTime -= sleepTime;
      }
      ready = 0;
      for (byte[] id : objectIds) {
        if (data.containsKey(new ObjectId(id))) {
          ready += 1;
        }
      }
      firstCheck = false;
    }
    ArrayList<ObjectStoreData> rets = new ArrayList<>();
    for (byte[] objId : objectIds) {
      ObjectId objectId = new ObjectId(objId);
      rets.add(new ObjectStoreData(metadata.get(objectId), data.get(objectId)));
    }
    return rets;
  }

  @Override
  public byte[] hash(byte[] objectId) {
    return null;
  }

  @Override
  public long evict(long numBytes) {
    return 0;
  }

  @Override
  public void release(byte[] objectId) {
    return;
  }

  @Override
  public void delete(byte[] objectId) {
    return;
  }

  @Override
  public boolean contains(byte[] objectId) {
    return data.containsKey(new ObjectId(objectId));
  }

  private String logPrefix() {
    return runtime.getWorkerContext().getCurrentTaskId() + "-" + getUserTrace() + " -> ";
  }

  private String getUserTrace() {
    StackTraceElement[] stes = Thread.currentThread().getStackTrace();
    int k = 1;
    while (stes[k].getClassName().startsWith("org.ray")
        && !stes[k].getClassName().contains("test")) {
      k++;
    }
    return stes[k].getFileName() + ":" + stes[k].getLineNumber();
  }

  public boolean isObjectReady(ObjectId id) {
    return data.containsKey(id);
  }

  public void free(ObjectId id) {
    data.remove(id);
    metadata.remove(id);
  }
}
