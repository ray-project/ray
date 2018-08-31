package org.ray.spi;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.id.UniqueId;
import org.ray.core.Serializer;
import org.ray.core.WorkerContext;
import org.ray.util.exception.TaskExecutionException;

/**
 * Object store proxy, which handles serialization and deserialization, and utilize a {@code
 * org.ray.spi.ObjectStoreLink} to actually store data.
 */
public class ObjectStoreProxy {

  private final ObjectStoreLink store;
  private final LocalSchedulerLink localSchedulerLink;
  private final int getTimeoutMs = 1000;

  public ObjectStoreProxy(ObjectStoreLink store) {
    this.store = store;
    this.localSchedulerLink = null;
  }

  public ObjectStoreProxy(ObjectStoreLink store, LocalSchedulerLink localSchedulerLink) {
    this.store = store;
    this.localSchedulerLink = localSchedulerLink;
  } 

  public <T> Pair<T, GetStatus> get(UniqueId objectId, boolean isMetadata)
      throws TaskExecutionException {
    return get(objectId, getTimeoutMs, isMetadata);
  }

  public <T> Pair<T, GetStatus> get(UniqueId id, int timeoutMs, boolean isMetadata)
      throws TaskExecutionException {
    byte[] obj = store.get(id.getBytes(), timeoutMs, isMetadata);
    if (obj != null) {
      T t = Serializer.decode(obj, WorkerContext.currentClassLoader());
      store.release(id.getBytes());
      if (t instanceof TaskExecutionException) {
        throw (TaskExecutionException) t;
      }
      return Pair.of(t, GetStatus.SUCCESS);
    } else {
      return Pair.of(null, GetStatus.FAILED);
    }
  }

  public <T> List<Pair<T, GetStatus>> get(List<UniqueId> objectIds, boolean isMetadata)
      throws TaskExecutionException {
    return get(objectIds, getTimeoutMs, isMetadata);
  }

  public <T> List<Pair<T, GetStatus>> get(List<UniqueId> ids, int timeoutMs, boolean isMetadata)
      throws TaskExecutionException {
    List<byte[]> objs = store.get(getIdBytes(ids), timeoutMs, isMetadata);
    List<Pair<T, GetStatus>> ret = new ArrayList<>();
    for (int i = 0; i < objs.size(); i++) {
      byte[] obj = objs.get(i);
      if (obj != null) {
        T t = Serializer.decode(obj, WorkerContext.currentClassLoader());
        store.release(ids.get(i).getBytes());
        if (t instanceof TaskExecutionException) {
          throw (TaskExecutionException) t;
        }
        ret.add(Pair.of(t, GetStatus.SUCCESS));
      } else {
        ret.add(Pair.of(null, GetStatus.FAILED));
      }
    }
    return ret;
  }

  private static byte[][] getIdBytes(List<UniqueId> objectIds) {
    int size = objectIds.size();
    byte[][] ids = new byte[size][];
    for (int i = 0; i < size; i++) {
      ids[i] = objectIds.get(i).getBytes();
    }
    return ids;
  }

  public void put(UniqueId id, Object obj, Object metadata) {
    store.put(id.getBytes(), Serializer.encode(obj), Serializer.encode(metadata));
  }

  public <T> WaitResult<T> wait(List<RayObject<T>> waitfor, int numReturns, int timeout) {
    List<UniqueId> ids = new ArrayList<>();
    for (RayObject<T> obj : waitfor) {
      ids.add(obj.getId());
    }
    List<byte[]> readys;
    if (localSchedulerLink == null) {
      readys = store.wait(getIdBytes(ids), timeout, numReturns);
    } else {
      readys = localSchedulerLink.wait(getIdBytes(ids), timeout, numReturns);
    }

    List<RayObject<T>> readyList = new ArrayList<>();
    List<RayObject<T>> unreadyList = new ArrayList<>();
    for (RayObject<T> obj : waitfor) {
      if (readys.contains(obj.getId().getBytes())) {
        readyList.add(obj);
      } else {
        unreadyList.add(obj);
      }
    }

    return new WaitResult<>(readyList, unreadyList);
  }

  public void fetch(List<UniqueId> objectIds) {
    if (localSchedulerLink == null) {
      store.fetch(getIdBytes(objectIds));
    } else {
      localSchedulerLink.reconstructObjects(objectIds, true);
    }
  }

  public enum GetStatus {
    SUCCESS, FAILED
  }
}
