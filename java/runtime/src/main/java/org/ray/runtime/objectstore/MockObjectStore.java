package org.ray.runtime.objectstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.ray.api.id.UniqueId;
import org.ray.runtime.RayDevRuntime;
import org.ray.runtime.raylet.MockRayletClient;
import org.ray.runtime.util.logger.RayLog;

/**
 * A mock implementation of {@code org.ray.spi.ObjectStoreLink}, which use Map to store data.
 */
public class MockObjectStore implements ObjectStoreLink {

  private final RayDevRuntime runtime;
  private final Map<UniqueId, byte[]> data = new ConcurrentHashMap<>();
  private final Map<UniqueId, byte[]> metadata = new ConcurrentHashMap<>();
  private MockRayletClient scheduler = null;

  public MockObjectStore(RayDevRuntime runtime) {
    this.runtime = runtime;
  }

  @Override
  public void put(byte[] objectId, byte[] value, byte[] metadataValue) {
    if (objectId == null || objectId.length == 0 || value == null) {
      RayLog.core
          .error(logPrefix() + "cannot put null: " + objectId + "," + Arrays.toString(value));
      System.exit(-1);
    }
    UniqueId uniqueId = new UniqueId(objectId);
    data.put(uniqueId, value);
    metadata.put(uniqueId, metadataValue);

    if (scheduler != null) {
      scheduler.onObjectPut(uniqueId);
    }
  }

  @Override
  public List<byte[]> get(byte[][] objectIds, int timeoutMs, boolean isMetadata) {
    final Map<UniqueId, byte[]> dataMap = isMetadata ? metadata : data;
    ArrayList<byte[]> rets = new ArrayList<>(objectIds.length);
    for (byte[] objId : objectIds) {
      UniqueId uniqueId = new UniqueId(objId);
      RayLog.core.info(logPrefix() + " is notified for objectid " + uniqueId);
      rets.add(dataMap.get(uniqueId));
    }
    return rets;
  }

  @Override
  public List<byte[]> wait(byte[][] objectIds, int timeoutMs, int numReturns) {
    ArrayList<byte[]> rets = new ArrayList<>();
    for (byte[] objId : objectIds) {
      //tod test
      if (data.containsKey(new UniqueId(objId))) {
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

  @Override
  public void release(byte[] objectId) {
    return;
  }

  @Override
  public boolean contains(byte[] objectId) {

    return data.containsKey(new UniqueId(objectId));
  }

  private String logPrefix() {
    return runtime.getWorkerContext().getCurrentTask().taskId + "-" + getUserTrace() + " -> ";
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

  public boolean isObjectReady(UniqueId id) {
    return data.containsKey(id);
  }

  public void registerScheduler(MockRayletClient s) {
    scheduler = s;
  }
}
