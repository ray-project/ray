package org.ray.spi;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.RayList;
import org.ray.api.RayObject;
import org.ray.api.UniqueID;
import org.ray.api.WaitResult;
import org.ray.core.Serializer;
import org.ray.core.WorkerContext;
import org.ray.util.exception.TaskExecutionException;

/**
 * Object store proxy, which handles serialization and deserialization, and utilize a {@code
 * org.ray.spi.ObjectStoreLink} to actually store data.
 */
public class ObjectStoreProxy {

  public enum GetStatus {SUCCESS, FAILED}

  private final ObjectStoreLink store;

  private final int GET_TIMEOUT_MS = 1000;

  public ObjectStoreProxy(ObjectStoreLink store) {
    this.store = store;
  }

  public <T> Pair<T, GetStatus> get(UniqueID id, int timeout_ms, boolean isMetadata)
      throws TaskExecutionException {
    byte[] obj = store.get(id.getBytes(), timeout_ms, isMetadata);
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

  public <T> Pair<T, GetStatus> get(UniqueID objectId, boolean isMetadata)
      throws TaskExecutionException {
    return get(objectId, GET_TIMEOUT_MS, isMetadata);
  }

  public <T> List<Pair<T, GetStatus>> get(List<UniqueID> ids, int timeoutMs, boolean isMetadata)
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

  public <T> List<Pair<T, GetStatus>> get(List<UniqueID> objectIds, boolean isMetadata)
      throws TaskExecutionException {
    return get(objectIds, GET_TIMEOUT_MS, isMetadata);
  }

  public void put(UniqueID id, Object obj, Object metadata) {
    store.put(id.getBytes(), Serializer.encode(obj), Serializer.encode(metadata));
  }

  public <T> WaitResult<T> wait(RayList<T> waitfor, int numReturns, int timeout) {
    List<UniqueID> ids = new ArrayList<>();
    for (RayObject<T> obj : waitfor.Objects()) {
      ids.add(obj.getId());
    }
    List<byte[]> readys = store.wait(getIdBytes(ids), timeout, numReturns);

    RayList<T> readyObjs = new RayList<>();
    RayList<T> remainObjs = new RayList<>();
    for (RayObject<T> obj : waitfor.Objects()) {
      if (readys.contains(obj.getId().getBytes())) {
        readyObjs.add(obj);
      } else {
        remainObjs.add(obj);
      }
    }

    return new WaitResult<>(readyObjs, remainObjs);
  }

  public void fetch(UniqueID objectId) {
    store.fetch(objectId.getBytes());
  }

  public void fetch(List<UniqueID> objectIds) {
    store.fetch(getIdBytes(objectIds));
  }

  public int getFetchSize() {
    return 10000;
  }


  private static byte[][] getIdBytes(List<UniqueID> objectIds) {
    int size = objectIds.size();
    byte[][] ids = new byte[size][];
    for (int i = 0; i < size; i++) {
      ids[i] = objectIds.get(i).getBytes();
    }
    return ids;
  }
}
