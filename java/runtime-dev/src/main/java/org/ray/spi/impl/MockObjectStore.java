package org.ray.spi.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.plasma.ObjectStoreLink;
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
  public void put(byte[] objectId, byte[] value, byte[] metadataValue) {
    if (objectId == null || objectId.length == 0 || value == null) {
      RayLog.core
          .error(logPrefix() + "cannot put null: " + objectId + "," + Arrays.toString(value));
      System.exit(-1);
    }
    UniqueID uniqueID = new UniqueID(objectId);
    data_.put(uniqueID, value);
    metadata_.put(uniqueID, metadataValue);

    if (scheduler_ != null) {
      scheduler_.onObjectPut(uniqueID);
    }
  }

  @Override
  public List<byte[]> get(byte[][] objectIds, int timeoutMs, boolean isMetadata) {
    final Map<UniqueID, byte[]> dataMap = isMetadata ? metadata_ : data_;
    ArrayList<byte[]> rets = new ArrayList<>(objectIds.length);
    for (byte[] objId : objectIds) {
      UniqueID uniqueID = new UniqueID(objId);
      RayLog.core.info(logPrefix() + " is notified for objectid " + uniqueID);
      rets.add(dataMap.get(uniqueID));
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
  public List<byte[]> wait(byte[][] objectIds, int timeoutMs, int numReturns) {
    ArrayList<byte[]> rets = new ArrayList<>();
    for (byte[] objId : objectIds) {
      //tod test
      if (data_.containsKey(new UniqueID(objId))) {
        rets.add(objId);
      }
    }
    return rets;
  }

  @Override
  public byte[] hash(byte[] objectId) {
    return null;
  }

  @Override
  public void fetch(byte[][] objectIds) {

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

  @Override
  public void release(byte[] objectId) {
    return;
  }

  @Override
  public boolean contains(byte[] objectId) {

    return data_.containsKey(new UniqueID(objectId));
  }
}
