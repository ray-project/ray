package org.ray.spi.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.plasma.ObjectId;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.plasma.ObjectBuffer;
import org.ray.api.UniqueID;
import org.ray.core.WorkerContext;
import org.ray.util.logger.RayLog;

/**
 * A mock implementation of {@code org.ray.spi.ObjectStoreLink}, which use Map to store data.
 */
public class MockObjectStore implements ObjectStoreLink {

  private final Map<UniqueID, byte[]> data_ = new ConcurrentHashMap<>();
  private final Map<UniqueID, byte[]> metadata_ = new ConcurrentHashMap<>();
  private MockLocalScheduler scheduler_ = null;

  @Override
  public void put(ObjectId objectId, byte[] value, byte[] metadataValue) {
    if (objectId == null || value == null) {
      RayLog.core
          .error(logPrefix() + "cannot put null: " + objectId + "," + Arrays.toString(value));
      System.exit(-1);
    }
    data_.put((UniqueID) objectId, value);
    metadata_.put((UniqueID) objectId, metadataValue);

    if (scheduler_ != null) {
      scheduler_.onObjectPut((UniqueID) objectId);
    }
  }

  @Override
  public List<ObjectBuffer> get(List<? extends ObjectId> objectIds, int timeoutMs, boolean isMetadata) {
    final Map<UniqueID, byte[]> dataMap = isMetadata ? metadata_ : data_;
    ArrayList<ObjectBuffer> rets = new ArrayList<>(objectIds.size());
    for (ObjectId objId : objectIds) {
      RayLog.core.info(logPrefix() + " is notified for objectid " + objId);
      rets.add(new ObjectBuffer(dataMap.get(objId)));
    }
    return rets;
  }

  private String logPrefix() {
    return WorkerContext.currentTask().taskId + "-" + getUserTrace() + " -> ";
  }

  private String getUserTrace() {
    StackTraceElement stes[] = Thread.currentThread().getStackTrace();
    int k = 1;
    while (stes[k].getClassName().startsWith("org.ray")
        && !stes[k].getClassName().contains("test")) {
      k++;
    }
    return stes[k].getFileName() + ":" + stes[k].getLineNumber();
  }

  @Override
  public List<ObjectId> wait(List<? extends ObjectId> objectIds, int timeoutMs, int numReturns) {
    ArrayList<ObjectId> rets = new ArrayList<>();
    for (ObjectId objId : objectIds) {
      if (data_.containsKey(objId)) {
        rets.add(objId);
      }
    }
    return rets;
  }

  @Override
  public byte[] hash(ObjectId objectId) {
    return null;
  }

  @Override
  public void fetch(List<? extends ObjectId> objectIds) {

  }

  @Override
  public long evict(long numBytes) {
    return 0;
  }

  public boolean isObjectReady(UniqueID id) {
    return data_.containsKey(id);
  }

  public void registerScheduler(MockLocalScheduler s) {
    scheduler_ = s;
  }
}
