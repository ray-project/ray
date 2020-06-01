package io.ray.runtime.object;

import io.ray.api.Ray;
import io.ray.api.RayObject;
import io.ray.api.id.ObjectId;
import java.io.Serializable;

/**
 * Implementation of {@link RayObject}.
 */
public final class RayObjectImpl<T> implements RayObject<T>, Serializable {

  private final ObjectId id;

  /**
   * Cache the result of `Ray.get()`.
   *
   * Note, this is necessary for direct calls, in which case, it's not allowed to call `Ray.get` on
   * the same object twice.
   */
  private transient T object;

  private Class<T> type;

  /**
   * Whether the object is already gotten from the object store.
   */
  private transient boolean objectGotten;

  public RayObjectImpl(ObjectId id, Class<T> type) {
    this.id = id;
    this.type = type;
    object = null;
    objectGotten = false;
  }

  @Override
  public synchronized T get() {
    if (!objectGotten) {
      object = Ray.get(id, type);
      objectGotten = true;
    }
    return object;
  }

  @Override
  public ObjectId getId() {
    return id;
  }

  @Override
  public Class<T> getType() {
    return type;
  }

}
