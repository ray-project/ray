package org.ray.core;

import java.io.Serializable;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.UniqueID;

public final class RayObjectImpl<T> implements RayObject<T>, Serializable {

  private final UniqueID id;
  private T object;
  private boolean local;

  public RayObjectImpl(UniqueID id) {
    this.id = id;
    object = null;
    local = false;
  }

  @Override
  public T get() {
    object = Ray.get(id);
    local = true;
    return object;
  }

  @Override
  public UniqueID getId() {
    return id;
  }

  @Override
  public boolean isLocal() {
    return local;
  }
}
