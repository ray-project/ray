package org.ray.runtime.objectstore;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.exception.RayException;
import org.ray.api.id.UniqueId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.util.Serializer;
import org.ray.runtime.util.UniqueIdUtil;

/**
 * Object store proxy, which handles serialization and deserialization, and utilize a {@code
 * org.ray.spi.ObjectStoreLink} to actually store data.
 */
public class ObjectStoreProxy {

  private static final int GET_TIMEOUT_MS = 1000;

  private final AbstractRayRuntime runtime;
  private final ObjectStoreLink store;

  public ObjectStoreProxy(AbstractRayRuntime runtime, ObjectStoreLink store) {
    this.runtime = runtime;
    this.store = store;
  }

  public <T> Pair<T, GetStatus> get(UniqueId objectId, boolean isMetadata)
      throws RayException {
    return get(objectId, GET_TIMEOUT_MS, isMetadata);
  }

  public <T> Pair<T, GetStatus> get(UniqueId id, int timeoutMs, boolean isMetadata)
      throws RayException {
    byte[] obj = store.get(id.getBytes(), timeoutMs, isMetadata);
    if (obj != null) {
      T t = Serializer.decode(obj, runtime.getWorkerContext().getCurrentClassLoader());
      store.release(id.getBytes());
      if (t instanceof RayException) {
        throw (RayException) t;
      }
      return Pair.of(t, GetStatus.SUCCESS);
    } else {
      return Pair.of(null, GetStatus.FAILED);
    }
  }

  public <T> List<Pair<T, GetStatus>> get(List<UniqueId> objectIds, boolean isMetadata)
      throws RayException {
    return get(objectIds, GET_TIMEOUT_MS, isMetadata);
  }

  public <T> List<Pair<T, GetStatus>> get(List<UniqueId> ids, int timeoutMs, boolean isMetadata)
      throws RayException {
    List<byte[]> objs = store.get(UniqueIdUtil.getIdBytes(ids), timeoutMs, isMetadata);
    List<Pair<T, GetStatus>> ret = new ArrayList<>();
    for (int i = 0; i < objs.size(); i++) {
      byte[] obj = objs.get(i);
      if (obj != null) {
        T t = Serializer.decode(obj, runtime.getWorkerContext().getCurrentClassLoader());
        store.release(ids.get(i).getBytes());
        if (t instanceof RayException) {
          throw (RayException) t;
        }
        ret.add(Pair.of(t, GetStatus.SUCCESS));
      } else {
        ret.add(Pair.of(null, GetStatus.FAILED));
      }
    }
    return ret;
  }

  public void put(UniqueId id, Object obj, Object metadata) {
    store.put(id.getBytes(), Serializer.encode(obj), Serializer.encode(metadata));
  }

  public void putSerialized(UniqueId id, byte[] obj, byte[] metadata) {
    store.put(id.getBytes(), obj, metadata);
  }

  public enum GetStatus {
    SUCCESS, FAILED
  }
}
