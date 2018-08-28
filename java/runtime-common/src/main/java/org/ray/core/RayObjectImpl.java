package org.ray.core;

import java.io.Serializable;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.UniqueID;

public final class RayObjectImpl<T> implements RayObject<T>, Serializable {

  private final UniqueID id;

  public RayObjectImpl(UniqueID id) {
    this.id = id;
  }

  @Override
  public T get() {
    return Ray.get(id);
  }

  @Override
  public UniqueID getId() {
    return id;
  }

}
