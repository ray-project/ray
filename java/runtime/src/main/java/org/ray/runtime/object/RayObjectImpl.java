package org.ray.runtime.object;

import java.io.Serializable;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.id.ObjectId;

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

  /**
   * Whether the object is already gotten from the object store.
   */
  private transient boolean objectGotten;

  public RayObjectImpl(ObjectId id) {
    this.id = id;
    object = null;
    objectGotten = false;
  }

  @Override
  public synchronized T get() {
    if (!objectGotten) {
      object = Ray.get(id);
      objectGotten = true;
    }
    return object;
  }

  @Override
  public ObjectId getId() {
    return id;
  }

}
