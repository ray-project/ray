package org.ray.runtime;

import java.io.Serializable;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.exception.RayException;
import org.ray.api.id.UniqueId;

public final class RayObjectImpl<T> implements RayObject<T>, Serializable {

  private final UniqueId id;

  public RayObjectImpl(UniqueId id) {
    this.id = id;
  }

  @Override
  public T get() {
    Object res = Ray.get(id);
    if (res instanceof RayException) {
      throw (RayException) res;
    }
    return (T) res;
  }

  @Override
  public UniqueId getId() {
    return id;
  }

}
