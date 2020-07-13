package io.ray.streaming.runtime.barrier;

import java.io.Serializable;
import java.util.Objects;

import com.google.common.base.MoreObjects;

public final class Barrier implements Serializable {

  private final long id;

  public Barrier(long id) {
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Barrier barrier = (Barrier) o;
    return id == barrier.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .toString();
  }

  public long getId() {
    return id;
  }
}
