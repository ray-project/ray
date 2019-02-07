package org.ray.runtime.objectstore;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.exception.RayException;
import org.ray.api.id.UniqueId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.RayDevRuntime;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.util.Serializer;
import org.ray.runtime.util.UniqueIdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object store proxy, which handles serialization and deserialization, and utilize a {@code
 * org.ray.spi.ObjectStoreLink} to actually store data.
 */
public class ObjectStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStoreProxy.class);

  private static final int GET_TIMEOUT_MS = 1000;

  private final AbstractRayRuntime runtime;

  private static ThreadLocal<ObjectStoreLink> objectStore;

  public ObjectStoreProxy(AbstractRayRuntime runtime, String storeSocketName) {
    this.runtime = runtime;
    objectStore = ThreadLocal.withInitial(() -> {
      if (runtime.getRayConfig().runMode == RunMode.CLUSTER) {
        return new PlasmaClient(storeSocketName, "", 0);
      } else {
        return ((RayDevRuntime) runtime).getObjectStore();
      }
    });
  }

  public <T> Pair<T, GetStatus> get(UniqueId objectId, boolean isMetadata)
      throws RayException {
    return get(objectId, GET_TIMEOUT_MS, isMetadata);
  }

  public <T> Pair<T, GetStatus> get(UniqueId id, int timeoutMs, boolean isMetadata)
      throws RayException {
    byte[] obj = objectStore.get().get(id.getBytes(), timeoutMs, isMetadata);
    if (obj != null) {
      T t = Serializer.decode(obj, runtime.getWorkerContext().getCurrentClassLoader());
      objectStore.get().release(id.getBytes());
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
    List<byte[]> objs = objectStore.get().get(UniqueIdUtil.getIdBytes(ids), timeoutMs, isMetadata);
    List<Pair<T, GetStatus>> ret = new ArrayList<>();
    for (int i = 0; i < objs.size(); i++) {
      byte[] obj = objs.get(i);
      if (obj != null) {
        T t = Serializer.decode(obj, runtime.getWorkerContext().getCurrentClassLoader());
        objectStore.get().release(ids.get(i).getBytes());
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
    try {
      objectStore.get().put(id.getBytes(), Serializer.encode(obj), Serializer.encode(metadata));
    } catch (DuplicateObjectException e) {
      LOGGER.warn(e.getMessage());
    }
  }

  public void putSerialized(UniqueId id, byte[] obj, byte[] metadata) {
    try {
      objectStore.get().put(id.getBytes(), obj, metadata);
    } catch (DuplicateObjectException e) {
      LOGGER.warn(e.getMessage());
    }
  }

  public enum GetStatus {
    SUCCESS, FAILED
  }
}
