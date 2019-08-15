package org.ray.runtime;

import java.io.Serializable;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.id.ObjectId;

public final class RayObjectImpl<T> implements RayObject<T>, Serializable {

  private final ObjectId id;

  public RayObjectImpl(ObjectId id) {
    this.id = id;
  }

  @Override
  public T get() {
    return Ray.get(id);
  }

  @Override
  public ObjectId getId() {
    return id;
  }

}
