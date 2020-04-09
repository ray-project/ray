package io.ray.streaming.runtime.core.common;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.UUID;
import org.ray.streaming.runtime.core.resource.ContainerID;

/**
 * Streaming system unique identity base class.
 * For example, ${@link io.ray.streaming.runtime.core.resource.ContainerID }
 */
public class AbstractID implements Serializable {
  private UUID id;

  public AbstractID() {
    this.id = UUID.randomUUID();
  }

  @Override
  public boolean equals(Object obj) {
    return id.equals(((AbstractID)obj).getId());
  }

  public UUID getId() {
    return id;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .toString();
  }
}
