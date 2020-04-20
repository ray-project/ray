package org.ray.runtime.group;

import org.ray.api.Bundle;
import org.ray.api.id.GroupId;

public class NativeBundle implements Bundle {

  private final GroupId id;

  public NativeBundle(GroupId id) {
    this.id = id;
  }

  @Override
  public GroupId getId() {
    return id;
  }
}
