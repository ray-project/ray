package org.ray.core;

import java.io.Serializable;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.id.UniqueId;

public final class RayObjectImpl<T> implements RayObject<T>, Serializable {

  private final UniqueId id;

  public RayObjectImpl(UniqueId id) {
    this.id = id;
  }

  @Override
  public T get() {
    return Ray.get(id);
  }

  @Override
  public UniqueId getId() {
    return id;
  }

}
