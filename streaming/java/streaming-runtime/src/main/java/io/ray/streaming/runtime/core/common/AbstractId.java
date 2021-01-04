package io.ray.streaming.runtime.core.common;

import com.google.common.base.MoreObjects;
import io.ray.streaming.runtime.core.resource.ContainerId;
import java.io.Serializable;
import java.util.UUID;

/** Streaming system unique identity base class. For example, ${@link ContainerId } */
public class AbstractId implements Serializable {

  private UUID id;

  public AbstractId() {
    this.id = UUID.randomUUID();
  }

  @Override
  public boolean equals(Object obj) {
    return id.equals(((AbstractId) obj).getId());
  }

  public UUID getId() {
    return id;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", id).toString();
  }
}
